# langgraph_agent/utils/nodes.py
import re
import logging
from typing import List, Dict, Any
from langchain_core.messages import AIMessage
from langchain.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from langchain.output_parsers import PydanticOutputParser
from langchain_core.exceptions import OutputParserException
from pydantic import BaseModel, Field
import pandas as pd

from utils.state import GraphState  # Import GraphState
from utils.tools import (
    get_procedure_ddl,
    get_table_ddl,
    execute_snowflake_query,
    execute_snowflake_dml,
    execute_snowflake_query_to_dataframe,
)
import os
from dotenv import load_dotenv

# Configure logging (optional, inherit from agent or configure separately)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
api_key = os.getenv("API_KEY")
if not api_key:
    logging.error("API_KEY environment variable not set!")
    raise ValueError("API_KEY environment variable not set!")

llm = ChatOpenAI(temperature=0, model_name="gpt-4o", api_key=api_key)

# --- Pydantic Models (Keep as is) ---
class TestCase(BaseModel):
    test_case: str = Field(..., description="A concise, descriptive name for the test case (e.g., 'Insert New Plant Record').")
    brief_description: str = Field(..., description="A brief explanation of the specific scenario being tested.")
    insert_query: str = Field(..., description="Complete and valid SQL INSERT query to load necessary data into the source table(s) before calling the procedure. Use sample values appropriate for column types. Exclude METADATA$ACTION if applicable. Provide 'N/A' if no insert is needed for this specific test.")
    source_table: str = Field(..., description="The fully qualified name (schema.table) of the primary table being inserted into by the insert_query. Provide 'N/A' if insert_query is 'N/A'.") # Added source_table
    expected_behaviour: str = Field(..., description="Describes the expected outcome of the procedure call for this test case (e.g., 'A new record should be inserted into the target table with specific values').")
    validation_query: str = Field(..., description="A complete and valid SQL SELECT COUNT(*) query against the target table to verify the expected behaviour after the procedure call. Provide 'N/A' if direct validation via COUNT(*) is not applicable.")
    expected_count: str = Field(..., description="The exact integer result (as a string, e.g., '1', '0') expected from the validation_query. Provide 'N/A' if validation_query is 'N/A'.")
    target_table: str = Field(..., description="The fully qualified name (schema.table) of the primary table being validated by the validation_query.")

class TestCasesList(BaseModel):
    test_cases: List[TestCase] = Field(..., description="A list containing exactly two distinct unit test case objects.")


# --- Node Functions ---

# --- extract_and_fetch_ddls (Keep the previously modified version) ---
def extract_and_fetch_ddls(state: GraphState) -> GraphState:
    """Extracts object names, infers their role (for tables), and fetches their DDLs."""
    procedure_ddl = state['procedure_ddl']
    ddl_data = state.get('ddl_data', []) # Use .get for safety
    messages = state.get('messages', [])

    logging.info("Starting DDL extraction, role inference, and fetching.")
    prompt_template = PromptTemplate(
        template="""
        You are a helpful assistant that extracts object names from Snowflake DDL and infers the role of tables.
        Given the following procedure DDL, Identify all tables, views, and sub-procedures referenced.

        For each object, determine its role based on how it's used in the procedure:
        - For TABLES: Infer if it's primarily read from ('source'), written to ('target'), or used for reference/lookups ('master'). Analyze INSERT/UPDATE/DELETE/MERGE statements for 'target' tables, and SELECT/JOIN clauses for 'source' or 'master' tables. If unsure, default to 'master'.
        - For PROCEDURES and VIEWS: Use 'N/A' as the role.

        Return the names, one per line, in the format:
        <object_type>:<full_object_name>:<role>

        Where:
        - <object_type> is 'TABLE', 'PROCEDURE', or 'VIEW'.
        - <full_object_name> is the fully qualified name (e.g., database.schema.table or database.schema.procedure_name). For procedures, do NOT include arguments or parentheses.
        - <role> is 'source', 'target', 'master', or 'N/A' as determined above.

        Example Output Lines:
        TABLE:MY_DB.MY_SCHEMA.CUSTOMERS:master
        TABLE:MY_DB.MY_SCHEMA.SALES_RAW:source
        TABLE:MY_DB.MY_SCHEMA.SALES_AGG:target
        PROCEDURE:MY_DB.MY_SCHEMA.SUB_PROC:N/A
        VIEW:MY_DB.MY_SCHEMA.CUSTOMER_VIEW:N/A

        Important:
        Concerning the tables pay attention to the naming convention:
        ### Naming Conventions:
        1. Streams are named with the prefix "str_", followed by the source table name (source_table_name), and end with a number (e.g., "_01", "_02", etc.).
        2. Temporary tables are named with the prefix "tmp_", followed by the stream name (tmp_str_source_table_name_0)
        3. The correct table names must not contain the prefixes "str_" or "tmp_". They represent the actual source and target tables.
        4. Every object that starts with str_ or tmp_ is not a table.

        Procedure DDL:
        ```{ddl}```

        Return ONLY the list in the specified format, one entry per line.
        """,
        input_variables=["ddl"]
    )
    prompt = prompt_template.format_prompt(ddl=procedure_ddl)
    try:
        response = llm.invoke(prompt)
        messages.append(response)
        logging.debug(f"LLM response for DDL extraction and role inference:\n{response.content}")

        extracted_lines = response.content.strip().split('\n')
        extracted_lines = [line.strip() for line in extracted_lines if line.strip()]
        logging.info(f"Extracted potential objects with roles: {extracted_lines}")

        fetched_ddls = []
        for line in extracted_lines:
            try:
                parts = line.split(":", 2)
                if len(parts) != 3:
                    logging.warning(f"Skipping malformed line (expected 3 parts separated by ':'): {line}")
                    continue

                object_type, full_object_name, role = parts
                object_type = object_type.strip().upper()
                full_object_name = full_object_name.strip()
                role = role.strip().lower()

                if full_object_name.split('.')[-1].lower().startswith(('tmp_', 'str_')):
                    logging.info(f"Skipping likely temporary/stream object based on name convention: {full_object_name}")
                    continue

                ddl = ""
                if object_type in ["TABLE", "VIEW"]:
                    ddl = get_table_ddl(full_object_name)
                    if object_type == "VIEW" and role not in ['n/a', 'unknown']:
                         logging.warning(f"Assigning role '{role}' to VIEW '{full_object_name}'. Expected 'N/A'. Using 'N/A'.")
                         role = 'n/a'
                elif object_type == "PROCEDURE":
                     parts_proc = full_object_name.split('.')
                     if len(parts_proc) >= 2:
                         schema_name = parts_proc[-2]
                         proc_name = parts_proc[-1]
                         ddl = get_procedure_ddl(proc_name, schema_name)
                     else:
                         logging.warning(f"Could not parse schema/name from PROCEDURE: {full_object_name}")
                         ddl = f"Error: Could not parse schema/name from {full_object_name}"
                     if role not in ['n/a', 'unknown']:
                         logging.warning(f"Assigning role '{role}' to PROCEDURE '{full_object_name}'. Expected 'N/A'. Using 'N/A'.")
                         role = 'n/a'
                else:
                    messages.append(AIMessage(content=f"Unknown object type encountered: {object_type} for {full_object_name}"))
                    logging.warning(f"Unknown object type: {object_type} for {full_object_name}")
                    continue

                if ddl and "Error" not in ddl and "not found" not in ddl:
                    fetched_ddls.append({
                        "objname": full_object_name,
                        "objtype": object_type,
                        "objrole": role,
                        "objddl": ddl
                    })
                    logging.info(f"Successfully fetched DDL for {object_type}: {full_object_name} (Role: {role})")
                else:
                    error_msg = f"Failed to fetch DDL for {object_type} {full_object_name}: {ddl}"
                    messages.append(AIMessage(content=error_msg))
                    logging.warning(error_msg)
                    # Optionally add entry with error DDL
                    # fetched_ddls.append({
                    #     "objname": full_object_name,
                    #     "objtype": object_type,
                    #     "objrole": role,
                    #     "objddl": f"Error fetching DDL: {ddl}"
                    # })

            except ValueError as e:
                msg = f"Error processing line '{line}': {e}"
                messages.append(AIMessage(content=msg))
                logging.warning(msg)
                continue
            except Exception as e:
                 msg = f"Unexpected error processing line {line}: {e}"
                 messages.append(AIMessage(content=msg))
                 logging.error(msg, exc_info=True)
                 continue

        new_state = state.copy()
        new_state["ddl_data"] = fetched_ddls
        new_state["messages"] = messages
        logging.info(fetched_ddls)
        return new_state
    except Exception as e:
        logging.error(f"Error during DDL extraction API call or processing: {e}", exc_info=True)
        messages.append(AIMessage(content=f"Error during DDL extraction/role inference: {e}"))
        new_state = state.copy()
        new_state["messages"] = messages
        if "ddl_data" not in new_state:
            new_state["ddl_data"] = ddl_data
        return new_state


def generate_unit_tests(state: GraphState) -> GraphState:
    """Generates unit tests for the procedure using PydanticOutputParser, providing DDL context grouped by role."""
    procedure_name = state['procedure_name']
    procedure_schema = state['procedure_schema']
    procedure_ddl = state['procedure_ddl']
    ddl_data = state.get('ddl_data', []) # List of dicts with 'objname', 'objtype', 'objrole', 'objddl'
    messages = state.get('messages', [])
    unit_tests = []

    logging.info(f"Starting unit test generation for {procedure_schema}.{procedure_name} with role-grouped DDL context.")

    # --- START: Restructure DDL context (Keep as is) ---
    source_tables_context = []
    target_tables_context = []
    master_tables_context = []
    other_objects_context = [] # For Views, Procedures, or Tables with unclear roles

    for item in ddl_data:
        obj_name = item.get('objname', 'UNKNOWN_NAME')
        obj_type = item.get('objtype', 'UNKNOWN_TYPE').upper()
        obj_role = item.get('objrole', 'unknown').lower()
        obj_ddl = item.get('objddl', 'DDL not available')

        formatted_entry = f"{obj_name}:\n```sql\n{obj_ddl}\n```\n"

        if obj_type == "TABLE":
            if obj_role == "source":
                source_tables_context.append(formatted_entry)
            elif obj_role == "target":
                target_tables_context.append(formatted_entry)
            elif obj_role == "master":
                master_tables_context.append(formatted_entry)
            else:
                logging.warning(f"Table '{obj_name}' has role '{obj_role}'. Classifying as 'Other'.")
                other_objects_context.append(f"-- TABLE (Role: {obj_role})\n{formatted_entry}")
        elif obj_type in ["PROCEDURE", "VIEW"]:
             other_objects_context.append(f"-- {obj_type}\n{formatted_entry}")
        else:
            logging.warning(f"Object '{obj_name}' has unknown type '{obj_type}'. Classifying as 'Other'.")
            other_objects_context.append(f"-- {obj_type}\n{formatted_entry}")

    all_ddls_context = f"Procedure DDL ({procedure_schema}.{procedure_name}):\n```sql\n{procedure_ddl}\n```\n\n"
    if not ddl_data:
        all_ddls_context += "No related object DDLs were found.\n"
    else:
        all_ddls_context += "Related Object DDLs by Role/Type:\n\n"
        if source_tables_context:
            all_ddls_context += "**Source Tables:**\n" + "\n".join(source_tables_context) + "\n"
        if target_tables_context:
            all_ddls_context += "**Target Tables:**\n" + "\n".join(target_tables_context) + "\n"
        if master_tables_context:
            all_ddls_context += "**Master Tables:**\n" + "\n".join(master_tables_context) + "\n"
        if other_objects_context:
            all_ddls_context += "**Other Objects (Views/Procedures/Misc Tables):**\n" + "\n".join(other_objects_context) + "\n"
    # --- END: Restructure DDL context ---

    parser = PydanticOutputParser(pydantic_object=TestCasesList)

    # --- START: MODIFIED Prompt Template Description ---
    prompt_template = PromptTemplate(
        template="""
            You are an expert in writing unit tests for Snowflake stored procedures.
            Your task is to generate exactly TWO distinct and comprehensive unit test cases for the given Snowflake procedure based on its DDL and the structured DDLs of related objects provided below.

            The related object DDLs are grouped by their inferred role (Source, Target, Master) or type (Other Objects). Use this structure to understand the data flow and dependencies.

            Focus on testing:
            1. Basic functionality: Correct data insertion/handling. Involves inserting into 'Source Tables' and validating results in 'Target Tables'.
            2. Basic functionality: Correct data update. Involves inserting into 'Source Tables' in order to update the already inserted record in the target table.

            Procedure Name: {procedure_name}
            Procedure Schema: {procedure_schema}

            Context DDLs (Grouped by inferred role/type):
            {all_ddls_context}

            Important Instructions:
            *   Use the structured DDL context above (Source Tables, Target Tables) to infer meaningful test data based on column names and datatypes.
            *   Generate valid, complete INSERT statements for necessary **Source Tables** to set up the test conditions.
            *   **CRITICAL - TARGET TABLE:** The `insert_query` MUST target the actual source table (usually listed under the '**Source Tables:**' heading in the context), NOT any streams (e.g., `str_...`) or temporary tables (e.g., `tmp_...`). Simulate data *before* it reaches the stream by inserting into the underlying source table. The `source_table` field you provide MUST match the table targeted by your `insert_query`.
            *   **CRITICAL - DATA TYPES:** Pay **strict attention** to the column data types and scale defined in the DDL for the specific **Source Table** you are inserting into (provided in the context).
            *   While generating the Insert Query, consider that there are conditions (WHERE clause) in the extraction process (SELECT) in the procedure code. Ensure your sample data would meet these conditions.
            *   While generating Insert Query for (2. Basic functionality - Update Test), use the same values for the merge/join Keys used in the (1. Basic functionality - Insert Test) Insert Query, but change other values (respecting their data types!) to test the update logic.
            *   Provide the fully qualified name of the source table(s) primarily used in the INSERT query for setup.
            *   EXCLUDE `METADATA$ACTION` from INSERT statements if the target table is a regular table, not a stream.
            *   Use `SELECT COUNT(*)` queries for validation, usually against **Target** tables, to verify the expected outcome after the procedure call.
            *   Provide COMPLETE, valid SQL.
            *   Provide the EXACT integer expected as a result of your validation query (as a string).
            *   If a section does not apply (e.g., no insert needed), respond with "N/A".
            *   The name of the target table for validation must be one of the tables listed in the DDL context, ideally identified as a 'Target Table'.

            {format_instructions}

            Generate the requested test cases according to the format instructions above.
        """
        ,
        input_variables=["procedure_name", "procedure_schema", "all_ddls_context"],
        partial_variables={"format_instructions": parser.get_format_instructions()}
    )
    # --- END: MODIFIED Prompt Template Description ---

    prompt = prompt_template.format_prompt(
        procedure_name=procedure_name,
        procedure_schema=procedure_schema,
        all_ddls_context=all_ddls_context # Pass the newly structured context
    )
    logging.info(f"Prompt sent to LLM for test generation:\n{prompt.to_string()}") # Log the prompt if needed

    try:
        response = llm.invoke(prompt)
        messages.append(response)
        llm_output = response.content
        logging.info("Received LLM response for unit tests.")
        logging.debug(f"LLM Raw Output:\n{llm_output}")

        parsed_output: TestCasesList = parser.parse(llm_output)
        unit_tests = [tc.dict() for tc in parsed_output.test_cases]
        logging.info(f"Successfully parsed {len(unit_tests)} unit tests.")
        logging.debug(f"Parsed unit tests: {unit_tests}")

    except OutputParserException as e:
        error_msg = f"Failed to parse LLM output into structured test cases: {e}"
        logging.error(error_msg, exc_info=True)
        messages.append(AIMessage(content=f"{error_msg}\nRaw LLM Output:\n{llm_output}"))
    except Exception as e:
        error_msg = f"An unexpected error occurred during test generation: {e}"
        logging.error(error_msg, exc_info=True)
        messages.append(AIMessage(content=error_msg))

    new_state = state.copy()
    new_state["unit_tests"] = unit_tests
    new_state["messages"] = messages
    return new_state

def execute_and_verify_tests(state: GraphState) -> GraphState:
    """Executes the validation queries, captures source/target data, and verifies results."""
    unit_tests = state.get('unit_tests', [])
    test_results = [] # Initialize fresh results list to store complex dicts
    messages = state.get('messages', [])
    procedure_name = state['procedure_name']
    procedure_schema = state['procedure_schema']
    # Get truncated status, default to False if not present
    truncated_tables = state.get('truncated_tables', set()) # Use a set to track truncated tables

    logging.info(f"Starting execution and verification of {len(unit_tests)} tests.")

    if not unit_tests:
        logging.warning("No unit tests were generated or parsed. Skipping execution.")
        messages.append(AIMessage(content="No unit tests available to execute."))
        # Ensure state keys are preserved even if returning early
        new_state = state.copy()
        new_state["test_results"] = test_results
        new_state["messages"] = messages
        new_state["truncated_tables"] = truncated_tables # Preserve the set
        return new_state

    # --- Identify unique SOURCE and TARGET tables to truncate ---
    # Use sets to automatically handle duplicates
    source_tables_to_truncate = {
        tc.get('source_table') for tc in unit_tests
        if tc.get('source_table') and tc.get('source_table') != "N/A"
    }
    target_tables_to_truncate = {
        tc.get('target_table') for tc in unit_tests
        if tc.get('target_table') and tc.get('target_table') != "N/A"
    }

    # Combine source and target tables, ensuring uniqueness
    all_tables_to_truncate = source_tables_to_truncate.union(target_tables_to_truncate)
    logging.info(f"Identified tables to potentially truncate: {all_tables_to_truncate}")

    # --- Truncate all identified tables ONCE before tests ---
    for table_to_truncate in all_tables_to_truncate:
        if table_to_truncate not in truncated_tables: # Check if already truncated in a previous run (if state persists)
            logging.info(f"Attempting to truncate table: {table_to_truncate}")
            truncate_query = f"TRUNCATE TABLE IF EXISTS {table_to_truncate};" # Use IF EXISTS for safety
            success = execute_snowflake_dml(truncate_query)
            if success:
                messages.append(AIMessage(content=f"Successfully truncated {table_to_truncate} before test execution."))
                logging.info(f"Successfully truncated {table_to_truncate}.")
                truncated_tables.add(table_to_truncate) # Mark as truncated for this run
            else:
                # Log error but continue execution, subsequent tests might be affected
                error_msg = f"Critical Error: Failed to truncate table {table_to_truncate} before test execution. Subsequent tests using this table may be unreliable."
                messages.append(AIMessage(content=error_msg))
                logging.error(error_msg)
        else:
            logging.info(f"Table {table_to_truncate} was already marked as truncated. Skipping.")


    # --- Execute each test case ---
    for i, test_case in enumerate(unit_tests):
        test_case_name = test_case.get('test_case', f'Unnamed Test {i+1}')
        insert_query = test_case.get('insert_query')
        source_table = test_case.get('source_table') # Get source table
        validation_query = test_case.get('validation_query')
        expected_count_str = test_case.get('expected_count')
        target_table = test_case.get('target_table')

        logging.info(f"Executing Test Case: {test_case_name}")

        # Initialize result dict for this test case
        current_test_result: Dict[str, Any] = {
            "test_case": test_case_name,
            "insert_query": insert_query or "N/A",
            "validation_query": validation_query or "N/A",
            "expected_count": expected_count_str or "N/A",
            "actual_count": "N/A",
            "result": "Skipped", # Default result
            "details": "Test not fully executed.",
            "source_data_before_sp": pd.DataFrame(), # Placeholder for source data
            "target_data_after_sp": pd.DataFrame() # Placeholder for target data
        }

        required_fields = [insert_query, source_table, validation_query, expected_count_str, target_table]
        if not all(f and f != "N/A" for f in required_fields): # Simplified check
            error_msg = f"Skipping test '{test_case_name}' due to missing or 'N/A' fields (insert_query, source_table, validation_query, expected_count, target_table)."
            logging.warning(error_msg)
            messages.append(AIMessage(content=error_msg))
            current_test_result["details"] = "Missing required fields for execution."
            test_results.append(current_test_result)
            continue

        if target_table not in truncated_tables:
            warn_msg = f"Warning: Test '{test_case_name}' targets table '{target_table}' which may not have been truncated successfully. Results might be inaccurate."
            logging.warning(warn_msg)
            messages.append(AIMessage(content=warn_msg))

        try:
            expected_count = int(expected_count_str)
            current_test_result["expected_count"] = expected_count

            execution_successful = True

            # 1. Execute Insert Query
            logging.debug(f"Executing Insert: {insert_query}")
            if not execute_snowflake_dml(insert_query):
                logging.error(f"Test '{test_case_name}': Insert query failed.")
                current_test_result["result"] = "Error"
                current_test_result["details"] = f"Failed to execute insert query: {insert_query}"
                messages.append(AIMessage(content=f"Test {test_case_name}: Insert query failed."))
                execution_successful = False
            else:
                logging.info(f"Test '{test_case_name}': Insert query executed successfully.")
                # 1b. Capture Source Table Data
                if execution_successful and source_table != "N/A":
                    select_source_query = f"SELECT * FROM {source_table};"
                    logging.debug(f"Capturing source data: {select_source_query}")
                    source_df = execute_snowflake_query_to_dataframe(select_source_query)
                    if source_df is not None:
                        current_test_result["source_data_before_sp"] = source_df
                        logging.info(f"Test '{test_case_name}': Captured source data from {source_table}.")
                    else:
                        logging.warning(f"Test '{test_case_name}': Failed to capture source data from {source_table}.")
                        current_test_result["details"] += f" Warning: Failed to capture source data after insert."

            # 2. Execute Stored Procedure
            if execution_successful:
                procedure_call = f"CALL {procedure_schema}.{procedure_name}();"
                logging.debug(f"Executing Procedure Call: {procedure_call}")
                if not execute_snowflake_dml(procedure_call):
                    logging.error(f"Test '{test_case_name}': Procedure call failed.")
                    current_test_result["result"] = "Error"
                    current_test_result["details"] = f"Failed to execute procedure call: {procedure_call}"
                    messages.append(AIMessage(content=f"Test {test_case_name}: Procedure call failed."))
                    execution_successful = False
                else:
                     logging.info(f"Test '{test_case_name}': Procedure call executed successfully.")
                     # 2b. Capture Target Table Data
                     if execution_successful and target_table != "N/A":
                         select_target_query = f"SELECT * FROM {target_table};"
                         logging.debug(f"Capturing target data: {select_target_query}")
                         target_df = execute_snowflake_query_to_dataframe(select_target_query)
                         if target_df is not None:
                             current_test_result["target_data_after_sp"] = target_df
                             logging.info(f"Test '{test_case_name}': Captured target data from {target_table}.")
                         else:
                             logging.warning(f"Test '{test_case_name}': Failed to capture target data from {target_table}.")
                             current_test_result["details"] += f" Warning: Failed to capture target data after SP call."

            # 3. Execute Validation Query
            actual_count = -1
            if execution_successful:
                logging.debug(f"Executing Validation: {validation_query}")
                actual_count = execute_snowflake_query(validation_query)
                current_test_result["actual_count"] = actual_count

                if actual_count == -1:
                     logging.error(f"Test '{test_case_name}': Validation query failed.")
                     current_test_result["result"] = "Error"
                     current_test_result["details"] = f"Failed to execute validation query: {validation_query}"
                     messages.append(AIMessage(content=f"Test {test_case_name}: Validation query failed."))
                     execution_successful = False
                else:
                    logging.info(f"Test '{test_case_name}': Validation query executed. Expected: {expected_count}, Actual: {actual_count}")

            # 4. Compare Results
            if execution_successful:
                result = "Pass" if actual_count == expected_count else "Fail"
                details = "Test passed." if result == "Pass" else f"Expected count {expected_count}, but got {actual_count}."
                current_test_result["result"] = result
                current_test_result["details"] = details
                messages.append(AIMessage(content=f"Test {test_case_name}: {result}"))
                logging.info(f"Test {test_case_name} Result: {result}")

        except ValueError:
            error_message = f"Test '{test_case_name}': Invalid non-integer value for expected_count: '{expected_count_str}'"
            logging.error(error_message)
            current_test_result["result"] = "Error"
            current_test_result["details"] = f"Invalid format for expected_count."
            current_test_result["expected_count"] = expected_count_str # Keep original string
            messages.append(AIMessage(content=error_message))
        except Exception as e:
            error_message = f"Test '{test_case_name}': An unexpected error occurred during execution: {e}"
            logging.error(error_message, exc_info=True)
            current_test_result["result"] = "Error"
            current_test_result["details"] = f"Unexpected execution error: {e}"
            messages.append(AIMessage(content=error_message))

        test_results.append(current_test_result)


    new_state = state.copy()
    new_state["test_results"] = test_results
    new_state["messages"] = messages
    new_state["truncated_tables"] = truncated_tables
    return new_state

