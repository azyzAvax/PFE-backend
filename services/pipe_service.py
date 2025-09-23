from fastapi import UploadFile, Form, BackgroundTasks, HTTPException
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from utils.excel_utils import (
    save_temp_file,
    remove_temp_file,
    extract_single_sheet_unstructured,
)
import logging
import os
import re
import traceback
from textwrap import dedent
from typing import List, Optional
from pydantic import BaseModel, Field
from fastapi.responses import FileResponse

try:
    import sqlparse
except ImportError:  # pragma: no cover - formatter optional at runtime
    sqlparse = None
from models.agent import (
    run_graph_for_api,
    ProcedureNotFoundError,
    GraphExecutionError,
    ReportCreationError,
    run_pipe_test_graph,
    PipeTestError,  # Import new exception
)

class ProcedureInput(BaseModel):
    procedure_name: str = Field(..., description="The name of the Snowflake stored procedure.")
    procedure_schema: str = Field(..., description="The schema of the Snowflake stored procedure.")
class PipeInput(BaseModel): # New model for Pipe testing
    pipe_name: str = Field(..., description="The name of the Snowflake Snowpipe.")
    pipe_schema: str = Field(..., description="The schema of the Snowflake Snowpipe.")

logger = logging.getLogger("uvicorn.error")

def init_llm(api_key: str) -> ChatOpenAI:
    """Initialize the LLM."""
    return ChatOpenAI(
        temperature=0,
        model_name="gpt-4o-mini",
        openai_api_key=api_key,
    )

def _clean_sql_output(sql_text: str) -> str:
    """Normalize SQL returned by the LLM (drop fences, keep indentation)."""
    if not sql_text:
        return ""

    cleaned = sql_text.replace("\r\n", "\n").strip()
    cleaned = re.sub(r"^```(?:sql)?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    formatted = dedent(cleaned).strip()

    # pipe_formatted = _format_pipe_sql(formatted)
    # if pipe_formatted:
    #     return pipe_formatted

    if sqlparse:
        formatted = sqlparse.format(
            formatted,
            keyword_case="upper",
            reindent=True,
            indent_width=2,
            strip_whitespace=True,
        )
        # sqlparse may add spaces before commas inside SELECT lists; tighten them.
        formatted = re.sub(r"\s+,", ",", formatted)

    return formatted




async def generate_pipe(file: UploadFile, sheet_name: str, env_vars: dict):
    """Generate Snowflake PIPE definition."""
    try:
        file_location = await save_temp_file(file, file.filename)
        extracted_data = extract_single_sheet_unstructured(file_location, sheet_name)
        print(extracted_data)

        llm = init_llm(env_vars["OPENAI_API_KEY"])
        template = """
You are an AI specializing in generating Snowflake PIPE definitions.
Analyze the provided file content from the sheet '{sheet_name}' and extract necessary details to construct a Snowflake PIPE.

### Required Fields:
- process_id: Use '{sheet_name}' as the process_id.
- LogicalName
- output_table_schema
- output_table_name
- output_table_zone
- file_format
- pattern_type
- stage_name
- From the "mapping" OR "Snowpipe" section (it may differ from sheet to sheet) : input_Name/Other and output_Name (column mappings)
and under the mapping area of the sheet you would find :
- type
- `input_Name/Other`: This maps to the values coming from the staged files (e.g., t.$1, t.$2, etc.)
- `output_Name`: This is the list of column names in the final target table.
### SQL Generation:
in case the mapping section contain 'update_at' field than put this value NULL in the SQL 
in case the mapping section contain 'if_file_name' or 'if_row_number' field than put this value METADATA$FILENAME , METADATA$FILE_ROW_NUMBER in the SQL respectfully
Generate a Snowflake PIPE with the following:
Return only the generated SQL query, with no additional text and no additional text like ```sql in begin.
CREATE OR REPLACE PIPE {{output_table_zone}}.{{process_id}}
INTEGRATION = 'NTF_INT_EVENTS'
AUTO_INGEST = TRUE
COMMENT = '{{logical_name}}'
AS
COPY INTO {{output_table_zone}}.{{output_table_name}} (
{{comma_separated_output_columns}}
)
FROM (
SELECT
{{input_Name/Other}} AS {{output_Name}} 
, '{{process_id}}' AS process_id
FROM
@{{stage_name}}/{{output_table_name}}/
(
FILE_FORMAT => '{{file_format}}',
PATTERN => '.*[.]{{pattern_type}}'
) t
); 
### the end of SQL Generation query
Extracted Excel Content:
{extracted_data}
"""

        prompt_template = PromptTemplate(
            template=template,
            input_variables=["extracted_data", "sheet_name"]
        )

        prompt = prompt_template.format(extracted_data=extracted_data, sheet_name=sheet_name)
        result = llm.invoke(prompt).content
        return result
    finally:
        remove_temp_file(file_location)

async def generate_pipe_with_json(json_data: dict, env_vars: dict = None):
    """Generate Snowflake PIPE definition from JSON input."""
    try:
        required_fields = [
            "process_id",
            "output_table_name",
            "output_table_zone",
            "file_format",
            "pattern_type",
            "stage_name",
            "mapping",
        ]
        for field in required_fields:
            if field not in json_data:
                raise KeyError(f"'{field}'")

        mapping = json_data["mapping"]
        if not isinstance(mapping, list):
            raise ValueError("'mapping' must be a list")
        for i, item in enumerate(mapping):
            if not isinstance(item, dict) or "output_Name" not in item:
                raise ValueError(f"'mapping' item at index {i} must be a dict with 'output_Name' key")

        column_list = ", ".join([item["output_Name"] for item in mapping])
        select_list = ", ".join([f"t.${i + 1} AS {item['output_Name']}" for i, item in enumerate(mapping)])

        constructed_sql = f"""
CREATE OR REPLACE PIPE {json_data["output_table_zone"]}.{json_data["process_id"]}
INTEGRATION = NTF_INT_EVENTS
AUTO_INGEST = TRUE
COMMENT = ''
AS
COPY INTO {json_data["output_table_zone"]}.{json_data["output_table_name"]} (
{column_list},
if_file_name,
if_row_number,
create_at,
update_at,
process_at,
process_id
)
FROM (
SELECT
{select_list},
METADATA$FILENAME AS if_file_name,
METADATA$FILE_ROW_NUMBER AS if_row_number,
CURRENT_TIMESTAMP() AS create_at,
NULL AS update_at,
CURRENT_TIMESTAMP() AS process_at,
'{json_data["process_id"]}' AS process_id
FROM
@{json_data["stage_name"]}/{json_data["output_table_name"]}/
(
FILE_FORMAT => '{json_data["file_format"]}',
PATTERN => '.*[.]{json_data["pattern_type"]}'
) t
);
"""

        return _clean_sql_output(constructed_sql)
    except Exception as e:
        error_msg = f"Internal server error: {str(e)}\nStack trace: {traceback.format_exc()}"
        logger.error(error_msg)
        raise

def remove_file(path: str) -> None:
    """Removes a file, logging errors if any."""
    try:
        os.remove(path)
        logging.info(f"Successfully removed temporary file: {path}")
    except OSError as e:
        logging.error(f"Error removing temporary file {path}: {e}", exc_info=True)

async def generate_report(
    input_data: ProcedureInput,
    background_tasks: BackgroundTasks # Inject background tasks for cleanup
):
    """
    API endpoint to trigger the unit test generation and reporting.
    """
    try:
        logging.info(f"Received request for {input_data.procedure_schema}.{input_data.procedure_name}")

        # Run your agent's core logic
        report_file_path = run_graph_for_api(
            procedure_name=input_data.procedure_name,
            procedure_schema=input_data.procedure_schema
        )

        # Ensure the file exists before attempting to send
        if not report_file_path or not os.path.exists(report_file_path):
             raise ReportCreationError("Report file path not generated or file does not exist.")

        # Schedule the temporary file to be deleted after the response is sent
        background_tasks.add_task(remove_file, report_file_path)

        # Return the file as a response
        # Construct a user-friendly filename for the download
        download_filename = f"{input_data.procedure_schema}_{input_data.procedure_name}_unit_test_report.xlsx"
        return FileResponse(
            path=report_file_path,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            filename=download_filename # Suggests filename to browser
        )

    except ProcedureNotFoundError as e:
        logging.warning(f"Procedure not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except (GraphExecutionError, ReportCreationError) as e:
        logging.error(f"Internal server error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {e}")
    except Exception as e:
        # Catch any other unexpected errors
        logging.error(f"Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")
    
async def test_snowpipe_endpoint(
    input_data: PipeInput,
    background_tasks: BackgroundTasks # Inject background tasks for cleanup
):
    """
    API endpoint to trigger the Snowpipe testing process and return an Excel report.
    """
    try:
        logging.info(f"Received request to test pipe {input_data.pipe_schema}.{input_data.pipe_name} and generate report.")

        report_file_path = run_pipe_test_graph(
            pipe_name=input_data.pipe_name,
            pipe_schema=input_data.pipe_schema
        )

        if not report_file_path or not os.path.exists(report_file_path):
             # This case should ideally be caught by exceptions in run_pipe_test_graph
             raise ReportCreationError("Pipe test report file path not generated or file does not exist.")

        background_tasks.add_task(remove_file, report_file_path)

        download_filename = f"{input_data.pipe_schema}_{input_data.pipe_name}_pipe_test_report.xlsx"
        return FileResponse(
            path=report_file_path,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            filename=download_filename
        )

    except (PipeTestError, ReportCreationError) as e:
        # Handle errors that occur during the pipe test graph or report creation.
        # These could indicate issues like the pipe not being found, Azure upload failing, etc.
        # Determine appropriate status code based on error message if possible.
        logging.error(f"Error during pipe test for {input_data.pipe_schema}.{input_data.pipe_name}: {e}", exc_info=True)
        status_code = 500
        if "not found" in str(e).lower() or "failed to get ddl" in str(e).lower():
            status_code = 404
        raise HTTPException(status_code=status_code, detail=str(e))
    except Exception as e:
        logging.error(f"Unexpected error during pipe test API call: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected internal server error occurred: {e}")
