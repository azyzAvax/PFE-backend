from fastapi import UploadFile
from fastapi.responses import JSONResponse
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from langgraph.graph import StateGraph
from langchain_core.runnables import RunnableLambda
from models.graph_state import GraphState
from utils.excel_utils import save_temp_file, remove_temp_file, extract_single_sheet_unstructured
from utils.env_utils import get_env_vars
import logging
import traceback
import json
import copy
import sys; sys.setrecursionlimit(2000);
from utils.llm_manager import  LLMSingleton
logger = logging.getLogger("uvicorn.error")
llm = LLMSingleton().get_llm()


usp_graph = StateGraph(GraphState)

def extract_metadata(state: GraphState, llm: ChatOpenAI) -> dict:
    """Extract metadata dynamically and update GraphState."""
    try:
        logger.info(f"GraphState before extraction: {state.__dict__}")
        metadata_prompt = PromptTemplate.from_template(
            """
You are an AI agent specializing in extracting process flows from structured Excel data for Snowflake USP generation.
Analyze the provided data and extract **metadata** and a **dynamic process flow**. 
Use the initial extracted data as the primary input.

Return ONLY a valid JSON object with the following structure, ensuring no extra text:

### **Extract the following metadata:**
- **General Information**: Process ID, Logical Name, Database Name, Schema Name, Input Table Schema, Process Type (set to 'USP' unless data specifies otherwise).
- **Procedure Name**: Snowflake Stored Procedure Name.
- **Input Table, Output Table, Temporary Table, Stream Name**: Physical and Logical Names, with schemas (default 'dlz' for Input/Temporary/Stream unless specified otherwise).

### **Dynamically Extract Process Flow Steps**
- Identify all steps in their intended execution order: initialization, data extraction, validation, merging/updating, and cleanup.
- For each step, include:
  - **Step Number**: Sequential order.
  - **Description**: Clear step purpose (e.g., "Data Extraction", "Validation - NULL Check").  
  - **Relevant Tables/Streams**: Source, temporary, or target tables/streams with physical names, logical names, and schemas.
  - **Column Names (Physical Names)**
  - **Logic to Implement**: Exact SQL commands for each step.
### **Additional Guidance**
- Ensure consistency with Snowflake conventions.

Extracted Excel Content:
{extracted_data}

### **Output Format**:
Provide the extracted metadata in **structured JSON format**, including all identified process steps.
"""
        )  #  extracted data holds the raw extracted data
        response = llm.invoke(metadata_prompt.format(extracted_data=state.extracted_data))

        logger.info(response.content)
        return {"metadata": response.content} 
    except Exception as e:
        logger.error(f"Error extracting metadata: {str(e)}\n{traceback.format_exc()}")
        raise

def generate_usp_template(state: GraphState, llm: ChatOpenAI) -> dict:
    """Generate an ordered USP SQL script from metadata in GraphState."""
    try:
        usp_template_prompt = PromptTemplate.from_template(
    """
You are an expert Snowflake SQL assistant. Based on the following extracted metadata and process_flow, generate a full User-Defined Stored Procedure (USP).

### Output Requirements:
- Use JavaScript syntax for the USP.
- Each step in `process_flow` should be implemented with a separate `snowflake.execute({{ sqlText: '<SQL_COMMAND>' }})`.
- Commands must be **ordered**.
- Add meaningful comments using the step's **Description**.
- Use `BEGIN; ... COMMIT;` for transaction handling.

### Example format:

```sql
CREATE OR REPLACE PROCEDURE {{{{Schema_name.procedure_name}}}}()
RETURNS OBJECT
LANGUAGE JAVASCRIPT
COMMENT = {{{{Logical_Name}}}}
EXECUTE AS CALLER
AS
$$
try {{
    // Setup
    snowflake.execute({{ sqlText: 'BEGIN;' }});
    snowflake.execute({{ sqlText: 'USE SCHEMA {{{{schema}}}};' }});

    // Step 1: Description
    snowflake.execute({{ sqlText: 'SQL_LOGIC_HERE' }});

    // Step 2: Description
    ...

    snowflake.execute({{ sqlText: 'COMMIT;' }});

    return {{ "status": "SUCCESS" }};
}}
catch (err) {{
    snowflake.execute({{ sqlText: 'ROLLBACK;' }});
    throw err;
}}
$$;
  
Metadata from GraphState:
{metadata}

### STRICT OUTPUT RULE:
The output must start with CREATE OR REPLACE PROCEDURE and end with the trailing semicolon (no extra ```sql in the beginning).
"""
        )
        response = llm.invoke(usp_template_prompt.format(metadata=json.dumps(state.metadata)))
        return {"usp_template": response.content.strip()}
    except Exception as e:
        logger.error(f"Error generating USP template: {str(e)}\n{traceback.format_exc()}")
        raise

usp_graph.add_node("extract_metadata", RunnableLambda(lambda state: extract_metadata(state, llm)))
usp_graph.add_node("generate_usp", RunnableLambda(lambda state: generate_usp_template(state, llm)))
usp_graph.add_edge("extract_metadata", "generate_usp")
usp_graph.set_entry_point("extract_metadata")
usp_workflow = usp_graph.compile()

async def generate_usp(file: UploadFile, sheet_name: str, env_vars: dict) -> dict:
    """Generate Snowflake USP from an Excel sheet."""
    file_location = await save_temp_file(file, file.filename)
    state = GraphState(extracted_data="")
    try:
        if not env_vars.get("OPENAI_API_KEY"):
            logger.error("OPENAI_API_KEY not found in env_vars")
            raise ValueError("OPENAI_API_KEY is missing")

        logger.info(f"API Key: {env_vars['OPENAI_API_KEY'][:10]}...")
        extracted_data = extract_single_sheet_unstructured(file_location, sheet_name)
        if not extracted_data:
            logger.error("Extracted data is empty")
            raise ValueError("No data extracted from the Excel sheet")
        logger.info(f"Extracted data content: {extracted_data}")

        state.extracted_data = extracted_data
        result = usp_workflow.invoke(state)
        return {
            "metadata": result.get("metadata", {}),
            "usp_template": result.get("usp_template", "")
        }
    except Exception as e:
        logger.error(f"Error in generate_usp: {str(e)}\n{traceback.format_exc()}")
        raise
    finally:
        remove_temp_file(file_location)