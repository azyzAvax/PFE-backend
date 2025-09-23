# langgraph_agent/utils/pipe_nodes.py
import logging
import re
import time
from typing import List, Dict, Any, Optional
from langchain_core.messages import AIMessage
from langchain.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from langchain.output_parsers import PydanticOutputParser
from langchain_core.exceptions import OutputParserException
from pydantic import BaseModel, Field
import pandas as pd

from utils.pipe_state import PipeGraphState # Import the new state
from utils.tools import (
    get_pipe_ddl,
    get_table_ddl,
    upload_csv_to_azure,
    execute_snowflake_query,
    execute_snowflake_query_to_dataframe, # Added for fetching full table data
)
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
api_key = os.getenv("API_KEY")
if not api_key:
    logging.error("API_KEY environment variable not set!")
    raise ValueError("API_KEY environment variable not set!")

# Use a capable model like gpt-4o for better instruction following and data generation
llm = ChatOpenAI(temperature=0.2, model_name="gpt-4o", api_key=api_key) # Increased temp slightly

# --- Pydantic Model for LLM CSV Output ---
class GeneratedCsvOutput(BaseModel):
    csv_content: str = Field(..., description="The generated CSV data as a single string, with rows separated by newlines and columns by commas. Include header row. Generate 3-5 rows of realistic data based on table DDL.")
    comment: str = Field(..., description="A brief comment about the generated data or any assumptions made.")

# --- Node Functions ---

def get_pipe_details(state: PipeGraphState) -> PipeGraphState:
    """Fetches Pipe DDL, extracts target table and Azure path, fetches Table DDL."""
    pipe_name = state['pipe_name']
    pipe_schema = state['pipe_schema']
    messages = state.get('messages', [])
    new_state = state.copy() # Start with a copy

    logging.info(f"Fetching DDL for pipe: {pipe_schema}.{pipe_name}")
    pipe_ddl = get_pipe_ddl(pipe_name, pipe_schema)

    if "Error" in pipe_ddl or "not found" in pipe_ddl:
        error_msg = f"Failed to get DDL for pipe '{pipe_schema}.{pipe_name}': {pipe_ddl}"
        logging.error(error_msg)
        new_state['error_message'] = error_msg
        new_state['messages'] = messages + [AIMessage(content=error_msg)]
        return new_state # Return early with error

    new_state['pipe_ddl'] = pipe_ddl
    messages.append(AIMessage(content=f"Successfully fetched Pipe DDL for {pipe_schema}.{pipe_name}."))
    logging.debug(f"Pipe DDL:\n{pipe_ddl}")

    # --- Extract Target Table and Azure Path using Regex (more reliable than LLM for this) ---
    target_table_name = None
    azure_folder_path = None

    # Regex for COPY INTO target_table
    copy_match = re.search(r"COPY\s+INTO\s+([a-zA-Z0-9_.\"$]+)", pipe_ddl, re.IGNORECASE | re.DOTALL)
    if copy_match:
        target_table_name = copy_match.group(1).strip().replace('"', '') # Remove quotes
        logging.info(f"Extracted target table: {target_table_name}")
        new_state['target_table_name'] = target_table_name
    else:
        error_msg = "Could not extract target table name from Pipe DDL using regex."
        logging.error(error_msg)
        new_state['error_message'] = error_msg
        new_state['messages'] = messages + [AIMessage(content=error_msg)]
        return new_state

    # Regex for FROM @stage/path/
    # Handles variations like @db.schema.stage/path or @schema.stage/path or @stage/path
    # Assumes path ends with '/' if it's a folder, or captures pattern if specified
    from_match = re.search(r"FROM\s+@([a-zA-Z0-9_.\"$]+)\/?([a-zA-Z0-9_.\/-]*)\/?", pipe_ddl, re.IGNORECASE | re.DOTALL)
    pattern_match = re.search(r"pattern\s*=>\s*'([^']*)'", pipe_ddl, re.IGNORECASE | re.DOTALL)

    if from_match:
        stage_name = from_match.group(1).strip()
        path_prefix = from_match.group(2).strip('/') if from_match.group(2) else ""
        azure_folder_path = path_prefix # The path within the stage
        logging.info(f"Extracted Azure folder path (relative to stage '{stage_name}'): {azure_folder_path}")
        new_state['azure_folder_path'] = azure_folder_path
        if pattern_match:
             logging.info(f"Pipe uses pattern: {pattern_match.group(1)}")
        else:
             logging.info("No specific file pattern found in pipe DDL.")
    else:
        error_msg = "Could not extract Azure folder path (FROM @.../path/) from Pipe DDL using regex."
        logging.error(error_msg)
        new_state['error_message'] = error_msg
        new_state['messages'] = messages + [AIMessage(content=error_msg)]
        return new_state

    # --- Fetch Target Table DDL ---
    logging.info(f"Fetching DDL for target table: {target_table_name}")
    table_ddl = get_table_ddl(target_table_name) # Assumes target_table_name is fully qualified or schema is clear

    if "Error" in table_ddl or "not found" in table_ddl:
        error_msg = f"Failed to get DDL for target table '{target_table_name}': {table_ddl}"
        logging.error(error_msg)
        # Don't necessarily fail the whole process, LLM might still generate *something*
        new_state['error_message'] = (new_state.get('error_message') or "") + "; " + error_msg
        messages.append(AIMessage(content=error_msg))
        # Let it proceed to CSV generation, but log the issue.
    else:
        messages.append(AIMessage(content=f"Successfully fetched Table DDL for {target_table_name}."))
        logging.debug(f"Table DDL:\n{table_ddl}")

    new_state['target_table_ddl'] = table_ddl
    new_state['messages'] = messages
    return new_state


def generate_csv_data(state: PipeGraphState) -> PipeGraphState:
    """Generates sample CSV data using LLM based on Pipe and Table DDL."""
    pipe_ddl = state.get('pipe_ddl')
    table_ddl = state.get('target_table_ddl')
    target_table_name = state.get('target_table_name')
    messages = state.get('messages', [])
    new_state = state.copy()

    if state.get('error_message'):
        logging.warning("Skipping CSV generation due to previous errors.")
        return new_state

    if not table_ddl or not pipe_ddl:
        error_msg = "Missing Pipe DDL or Table DDL for CSV generation."
        logging.error(error_msg)
        new_state['error_message'] = error_msg
        new_state['messages'] = messages + [AIMessage(content=error_msg)]
        return new_state

    parser = PydanticOutputParser(pydantic_object=GeneratedCsvOutput)

    prompt_template = PromptTemplate(
        template="""
            You are an expert data generator for Snowflake Snowpipe testing.
            Your task is to generate realistic sample CSV data (3-5 rows) that can be loaded via the given Snowpipe into the target table.

            Target Table Name: {target_table_name}
            Target Table DDL:
            ```sql
            {table_ddl}
            ```

            Snowpipe DDL (Pay attention to the column mapping in the COPY INTO statement, e.g., $1, $2...):
            ```sql
            {pipe_ddl}
            ```

            Instructions:
            1.  Analyze the Target Table DDL to understand column names and data types (VARCHAR, NUMBER, DATE, TIMESTAMP, etc.).
            2.  Analyze the Snowpipe DDL's `COPY INTO ... FROM (SELECT t.$1, t.$2 ...)` section to understand the mapping between CSV columns ($1, $2, ...) and table columns.
            3.  Generate CSV data where the number of columns matches the highest `$N` used in the Snowpipe's SELECT clause.
            4.  Create 3 to 5 rows of sample data. Ensure the data types are appropriate for the corresponding *target table columns* based on the Snowpipe mapping.
            5.  **Crucially**: Do NOT include columns in the CSV that are generated *by the pipe itself* (e.g., METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()). Only include columns corresponding to the `$N` placeholders.
            6.  Format the output as a standard CSV string:
                *   Include a header row with meaningful (even if simple like Col1, Col2) names based on the $N order.
                *   Use comma (,) as the delimiter.
                *   Use newline (\n) as the row separator.
                *   Enclose strings containing commas or newlines in double quotes (""). Handle quoting appropriately if needed, but aim for simple data initially.

            {format_instructions}

            Generate the CSV data according to the format instructions. Provide a brief comment.
        """,
        input_variables=["target_table_name", "table_ddl", "pipe_ddl"],
        partial_variables={"format_instructions": parser.get_format_instructions()}
    )

    prompt = prompt_template.format_prompt(
        target_table_name=target_table_name,
        table_ddl=table_ddl,
        pipe_ddl=pipe_ddl
    )
    logging.info("Sending prompt to LLM for CSV generation...")
    logging.debug(f"LLM Prompt:\n{prompt.to_string()}")

    try:
        response = llm.invoke(prompt)
        llm_output = response.content
        logging.info("Received LLM response for CSV generation.")
        logging.info(f"LLM Raw Output:\n{llm_output}")

        parsed_output: GeneratedCsvOutput = parser.parse(llm_output)

        csv_content = parsed_output.csv_content.strip()
        # Basic validation: check if it has headers and at least one data row
        if '\n' not in csv_content or not csv_content:
             raise ValueError("Generated CSV content seems empty or lacks structure.")

        new_state['generated_csv_content'] = csv_content
        new_state['generated_csv_filename'] = f"{state['pipe_name']}_test_{int(time.time())}.csv" # Unique filename
        messages.append(AIMessage(content=f"Successfully generated CSV data. Comment: {parsed_output.comment}"))
        logging.info(f"Generated CSV Filename: {new_state['generated_csv_filename']}")
        logging.debug(f"Generated CSV Content:\n{csv_content}")

    except (OutputParserException, ValueError) as e:
        error_msg = f"Failed to parse LLM output or validate generated CSV: {e}"
        logging.error(error_msg, exc_info=True)
        new_state['error_message'] = error_msg
        messages.append(AIMessage(content=f"{error_msg}\nRaw LLM Output:\n{llm_output}"))
    except Exception as e:
        error_msg = f"An unexpected error occurred during CSV generation: {e}"
        logging.error(error_msg, exc_info=True)
        new_state['error_message'] = error_msg
        messages.append(AIMessage(content=error_msg))

    new_state['messages'] = messages
    return new_state


def upload_and_verify_pipe(state: PipeGraphState) -> PipeGraphState:
    """Uploads the generated CSV to Azure, waits, and verifies data in the target table."""
    csv_content = state.get('generated_csv_content')
    filename = state.get('generated_csv_filename')
    azure_folder_path = state.get('azure_folder_path')
    target_table = state.get('target_table_name')
    messages = state.get('messages', [])
    new_state = state.copy()
    new_state['target_table_data_after_test'] = pd.DataFrame() # Initialize

    if state.get('error_message'): # If previous steps had critical errors
        logging.warning("Skipping upload and verification due to previous errors.")
        new_state["final_message"] = f"Test skipped due to errors: {state['error_message']}"
        return new_state

    if not all([csv_content, filename, azure_folder_path, target_table]):
        error_msg = "Missing required data for upload/verification (CSV content, filename, Azure path, or target table)."
        logging.error(error_msg)
        new_state['error_message'] = error_msg
        new_state["final_message"] = f"Test failed: {error_msg}"
        new_state['messages'] = messages + [AIMessage(content=error_msg)]
        return new_state

    # --- Upload to Azure ---
    logging.info(f"Uploading '{filename}' to Azure path '{azure_folder_path}'...")
    upload_success = upload_csv_to_azure(csv_content, azure_folder_path, filename)
    new_state['upload_status'] = upload_success

    if not upload_success:
        error_msg = f"Failed to upload CSV file '{filename}' to Azure."
        logging.error(error_msg)
        new_state['error_message'] = error_msg
        new_state["final_message"] = f"Test failed: {error_msg}"
        new_state['messages'] = messages + [AIMessage(content=error_msg)]
        return new_state
    else:
        messages.append(AIMessage(content=f"Successfully uploaded '{filename}' to Azure."))
        logging.info("Upload successful. Waiting for Snowpipe processing...")

    # --- Wait ---
    wait_time = 35 # Seconds
    logging.info(f"Waiting {wait_time} seconds...")
    time.sleep(wait_time)
    logging.info("Wait finished. Proceeding with verification.")

    # --- Verify Row Count ---
    verification_query = f"SELECT COUNT(*) FROM {target_table}"
    logging.info(f"Executing verification query (COUNT): {verification_query}")
    new_state['verification_query'] = verification_query
    actual_count = execute_snowflake_query(verification_query)

    if actual_count == -2: # Specific code for execution error from our tool
        error_msg = f"Failed to execute verification query (COUNT): {verification_query}"
        logging.error(error_msg)
        new_state['error_message'] = error_msg
        new_state["final_message"] = f"Test partially succeeded (upload OK), but verification (COUNT) failed: {error_msg}"
        new_state['verification_result'] = None
    elif actual_count == -1: # Specific code for query ran but returned no data (unexpected for COUNT(*))
         error_msg = f"Verification query (COUNT) ran but returned no data (unexpected): {verification_query}"
         logging.warning(error_msg)
         new_state['error_message'] = error_msg
         new_state["final_message"] = f"Test status uncertain: Upload OK, but verification query (COUNT) returned no data. Row count: 0."
         new_state['verification_result'] = 0
    else:
        num_generated_rows = len(csv_content.strip().split('\n')) - 1
        if num_generated_rows < 0: num_generated_rows = 0

        logging.info(f"Verification query (COUNT) successful. Row count in '{target_table}': {actual_count}.")
        new_state['verification_result'] = actual_count
        if actual_count > 0:
            final_msg_count = f"Found {actual_count} rows in '{target_table}' after waiting. (Generated {num_generated_rows} rows)."
            messages.append(AIMessage(content=f"Verification (COUNT) successful. {final_msg_count}"))
            new_state["final_message"] = f"Test successful: Upload OK. {final_msg_count}"
        else:
            final_msg_count = f"Found 0 rows in '{target_table}' after waiting. (Generated {num_generated_rows} rows)."
            logging.warning(f"Verification (COUNT): {final_msg_count}")
            messages.append(AIMessage(content=f"Verification (COUNT) shows potential issue: {final_msg_count}"))
            new_state["final_message"] = f"Test potentially failed: Upload OK, but {final_msg_count}"

    # --- Fetch Target Table Data (SELECT *) ---
    if upload_success: # Proceed to fetch data even if count is 0, to show an empty table
        select_all_query = f"SELECT * FROM {target_table}"
        logging.info(f"Fetching all data from target table: {select_all_query}")
        target_table_df = execute_snowflake_query_to_dataframe(select_all_query)

        if target_table_df is not None:
            new_state['target_table_data_after_test'] = target_table_df
            logging.info(f"Successfully fetched {len(target_table_df)} rows from '{target_table}'.")
            messages.append(AIMessage(content=f"Successfully fetched data from {target_table} for reporting."))
        else:
            logging.warning(f"Failed to fetch data from '{target_table}' using query: {select_all_query}. Report will show empty data.")
            messages.append(AIMessage(content=f"Warning: Failed to fetch data from {target_table} for reporting."))
            # new_state['target_table_data_after_test'] is already initialized to empty DataFrame

    new_state['messages'] = messages
    return new_state