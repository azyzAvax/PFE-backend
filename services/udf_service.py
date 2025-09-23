from fastapi import UploadFile, Form
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from langgraph.graph import StateGraph
from langchain_core.runnables import RunnableLambda
from models.graph_state import UDFGraphState
from utils.excel_utils import save_temp_file, remove_temp_file, extract_single_sheet_unstructured
import os
import logging
import traceback

logger = logging.getLogger("uvicorn.error")

def init_llm(api_key: str) -> ChatOpenAI:
    """Initialize the LLM."""
    return ChatOpenAI(
        temperature=0,
        model_name="gpt-4o-mini",
        openai_api_key=api_key,
    )

# Pre-compile the JavaScript UDF workflow
udf_js_graph = StateGraph(UDFGraphState)
udf_metadata_prompt = PromptTemplate.from_template("""
You are an AI agent specializing in extracting metadata and process flows from structured Excel data for Snowflake UDFs.
Analyze the provided data and extract the necessary information in a structured JSON format.

### **Extract the following metadata:**
- **General Information**
  - Process ID: Extract the function name as it appears in the document.
  - Logical Name: Extract the name used as the comment.
  - Schema Name: Extract the zone.
  - Process Type: Determine whether the UDF should be "SQL" or "JAVASCRIPT". If the document provides no explicit indication:
    - Prioritize "SQL" if the logic involves database queries (e.g., SELECT statements, aggregations like ARRAY_AGG) or table operations.
    - Default to "JAVASCRIPT" for custom logic (e.g., date manipulations, loops) that cannot be easily expressed in SQL.
    - Infer the type from any provided code examples or descriptions if available.
  - Input Parameters:
    - Extract all parameters from the "THE FOLLOWING ARGUMENTS ARE ASSUMED TO BE INPUT" section or equivalent.
    - For each parameter, include the physical name and data type (e.g., VARCHAR, DATE, ARRAY) as explicitly stated or implied by the document's functionality.
    - Include all parameters necessary to fulfill the UDF's purpose as described.
    - Return the parameters as a list of objects with keys "physical_name" and "data_type".
  - Output:
    - Extract the return type (e.g., DATE) from the "OUTPUT" section or equivalent.
    - Identify the physical name of the output variable (if applicable).
    - Return as an object with keys "physical_name" (if applicable) and "data_type".

### **Dynamically Extract Process Flow Steps**
- Analyze the document to identify the steps required to achieve the UDF's purpose, based solely on the described functionality.
- Structure the steps as a list of objects under the key "process_flow_steps".
- Derive the steps and their logic entirely from the document content, focusing on the intended behavior rather than any provided code examples that mix incompatible syntax.

#### **Each Step Should Have:**
- **step_number**: Assign a sequential identifier.
- **description**: A brief description of the step based on the document's intended functionality.
- **logic**: The code to implement the step, written as a string. Ensure the code:
  - Matches the "Process Type" determined in "general_information":
    - For "SQL" UDFs, use SQL syntax (e.g., SELECT, CASE, WITH clauses) compatible with Snowflake SQL UDFs.
    - For "JAVASCRIPT" UDFs, use only JavaScript-native syntax and methods, explicitly avoiding SQL functions or calls to external UDFs.
  - Reflects the operations and variable names implied by the document's purpose.
- **inputs**: The variables used as inputs for this step.
- **outputs**: The variables produced by this step.

### **Error Handling**
- If the document suggests input validation (e.g., for dates), include it as a separate step with appropriate logic based on the "Process Type".

Extracted Excel Content:
{extracted_data}

Provide the extracted metadata in **structured JSON format** with keys "general_information" and "process_flow_steps". Ensure the JSON is valid and properly formatted.
""")

def extract_udf_metadata(state: UDFGraphState, extracted_data: str, llm: ChatOpenAI) -> dict:
    """Extract metadata for UDF."""
    try:
        response = llm.invoke(udf_metadata_prompt.format(extracted_data=extracted_data))
        logger.info("Extracted UDF Metadata: %s", response.content)
        return {"metadata": response.content}
    except Exception as e:
        logger.error("Error extracting UDF metadata: %s\n%s", str(e), traceback.format_exc())
        raise

udf_js_template_prompt = PromptTemplate.from_template("""
You are an AI agent that generates Snowflake **User-Defined Functions (UDF)** based on extracted metadata.
Use the metadata to construct a valid UDF.

### **Generate a JavaScript UDF**
- Use the process flow steps extracted from metadata under the "process_flow_steps" key.
- The UDF type must be "JAVASCRIPT" as determined by the metadata.
- Each process step should be structured in the UDF in the order provided.

### **Specific Instructions**
- Use only JavaScript-native syntax and methods, DO NOT USE SQL functions inside of the javascript code, strictly prohibiting calls to external UDFs.
- Ensure the code: For JAVASCRIPT UDFs, uses only JavaScript-native syntax and methods, explicitly avoiding SQL functions or calls to external UDFs.
- When accessing input parameters in the JavaScript code, use the ****UPPERCASE**** version of the parameter names  as per Snowflake JavaScript UDF conventions.
- Ensure the logic matches the exact steps and variable names from the metadata, without modification or addition.

### **Output Formatting**
- Generate the UDF as a single-line query with no newline characters (`\n`).
- Organize the JavaScript logic clearly by placing each step's logic on the same line, separated by spaces, with proper semicolons between statements.
- Use let instead of var.
- DO NOT USE SQL functions inside of the javascript code.
- The output should be only the variable without oISOString().
Extracted Metadata & Process Flow (do not write it in the result response):
{metadata}

### **Generate UDF**
Provide a full **UDF** using this structure:
- "CREATE OR REPLACE FUNCTION {{schema_name}}.{{function_name}}({{input_parameters}}) 
    RETURNS {{return_type}} LANGUAGE JAVASCRIPT COMMENT ={{logical_name}} AS $$ {{logic}} return {{output_variable}}; $$;"


Provide only the UDF query result from the udf_template; do not provide the metadata extracted.
""")

def generate_js_udf_template(state: UDFGraphState, llm: ChatOpenAI) -> dict:
    """Generate JavaScript UDF script."""
    try:
        udf_sql = llm.invoke(udf_js_template_prompt.format(metadata=state.metadata))
        logger.info("Generated UDF SQL: %s", udf_sql.content)
        return {"udf_template": udf_sql.content}
    except Exception as e:
        logger.error("Error generating JS UDF template: %s\n%s", str(e), traceback.format_exc())
        raise

# Add nodes and edges to the pre-compiled graph
udf_js_graph.add_node("extract_metadata", RunnableLambda(lambda state: extract_udf_metadata(state, state.metadata, init_llm(os.getenv("OPENAI_API_KEY")))))
udf_js_graph.add_node("generate_js_udf", RunnableLambda(lambda state: generate_js_udf_template(state, init_llm(os.getenv("OPENAI_API_KEY")))))
udf_js_graph.add_edge("extract_metadata", "generate_js_udf")
udf_js_graph.set_entry_point("extract_metadata")
udf_js_workflow = udf_js_graph.compile()

# Pre-compile the SQL UDF workflow
udf_sql_graph = StateGraph(UDFGraphState)
udf_sql_template_prompt = PromptTemplate.from_template("""
You are an AI agent that generates Snowflake **User-Defined Functions (UDF)** based on extracted metadata.
Use the metadata to construct a valid UDF.
### **Generate an SQL UDF**
- Use the process flow steps extracted from metadata under the "process_flow_steps" key.
- Combine the process steps into a single SQL query using Common Table Expressions (CTEs) to structure the logic clearly, following the sequence of steps provided.

### **Specific Instructions**
- Adhere strictly to Snowflake SQL UDF conventions as per Snowflake documentation (e.g., https://docs.snowflake.com/en/sql-reference/user-defined-functions).
- **Parameter Naming**:
  - In the SQL body, **always** reference input parameters using their ***UPPERCASE*** names. This is a strict Snowflake SQL UDF requirement.
- **SQL Syntax**:
  - Use SQL functions and operators (e.g., SELECT, CASE, WITH, DATE_TRUNC...) to implement the logic.
  - For table aliases, **always** use the "AS" keyword (e.g., "FROM table_name AS alias") to ensure compliance with Snowflake best practices.
  - Structure the query using CTEs (WITH clauses) for each logical step in the metadata, ensuring each CTE builds on the previous one.
  - Avoid redundant calculations (e.g., repeating CASE statements for the same value) by reusing computed values across the query.
  - For month comparisons, use ***DATE_TRUNC('MONTH', column)*** to match dates.
- **VARIANT Output**:
  - For VARIANT returns, use OBJECT_CONSTRUCT with TO_VARIANT (e.g., TO_VARIANT(OBJECT_CONSTRUCT('key', value))).
  - **Prohibit** the use of PARSE_JSON with string concatenation, as it can lead to errors with non-string or NULL values.
  - Ensure all values in OBJECT_CONSTRUCT are properly typed (e.g., strings are quoted with single quotes, numbers are not).
- **Return Type**:
  - Return the type specified in the metadata's "general_information.output.data_type", using TO_VARIANT to wrap the OBJECT_CONSTRUCT result for VARIANT returns.
- For NUMBER, use direct aggregation (e.g., SUM/COUNT) with ROUND to match precision.
### **Output Formatting**
- Generate the UDF as a single-line query with no newline characters (`\n`).
- Organize the SQL logic clearly, ensuring proper nesting of CTEs and use of parentheses.
- Use the structure: "CREATE OR REPLACE FUNCTION {{schema_name}}.{{function_name}}({{input_parameters}}) RETURNS {{return_type}} LANGUAGE SQL COMMENT = '{{logical_name}}' AS $$ {{logic}} $$ ;"
- For `input_parameters`, format the parameters as a comma-separated list using the "physical_name" and "data_type" from the metadata's "general_information.input_parameters".
- For UDF's name use ProcessID.
- For `logic`, combine the "logic" from each step in "process_flow_steps" into a single SQL query, using CTEs to reflect the step sequence, with all parameter references in uppercase.

Extracted Metadata & Process Flow (do not write it in the result response):
{metadata}

### **Generate UDF**
Provide only the UDF query result from the udf_template; do not provide the metadata extracted.
""")

def generate_sql_udf_template(state: UDFGraphState, llm: ChatOpenAI) -> dict:
    """Generate SQL UDF script."""
    try:
        udf_sql = llm.invoke(udf_sql_template_prompt.format(metadata=state.metadata))
        logger.info("Generated SQL UDF: %s", udf_sql.content)
        return {"udf_template": udf_sql.content}
    except Exception as e:
        logger.error("Error generating SQL UDF template: %s\n%s", str(e), traceback.format_exc())
        raise

# Add nodes and edges to the pre-compiled graph
udf_sql_graph.add_node("extract_metadata", RunnableLambda(lambda state: extract_udf_metadata(state, state.metadata, init_llm(os.getenv("OPENAI_API_KEY")))))
udf_sql_graph.add_node("generate_sql_udf", RunnableLambda(lambda state: generate_sql_udf_template(state, init_llm(os.getenv("OPENAI_API_KEY")))))
udf_sql_graph.add_edge("extract_metadata", "generate_sql_udf")
udf_sql_graph.set_entry_point("extract_metadata")
udf_sql_workflow = udf_sql_graph.compile()

async def generate_js_udf(file: UploadFile, sheet_name: str, env_vars: dict) -> str:
    """Generate Snowflake JavaScript UDF from an Excel sheet."""
    file_location = await save_temp_file(file, file.filename)
    try:
        extracted_data = extract_single_sheet_unstructured(file_location, sheet_name)
        llm = init_llm(env_vars["OPENAI_API_KEY"])
        
        # Use pre-compiled workflow
        result = udf_js_workflow.invoke(UDFGraphState(metadata=extracted_data))
        return result["udf_template"]
    except Exception as e:
        logger.error("Error in generate_js_udf: %s\n%s", str(e), traceback.format_exc())
        raise
    finally:
        remove_temp_file(file_location)

async def generate_sql_udf(file: UploadFile, sheet_name: str, env_vars: dict) -> str:
    """Generate Snowflake SQL UDF from an Excel sheet."""
    file_location = await save_temp_file(file, file.filename)
    try:
        extracted_data = extract_single_sheet_unstructured(file_location, sheet_name)
        llm = init_llm(env_vars["OPENAI_API_KEY"])
        
        # Use pre-compiled workflow
        result = udf_sql_workflow.invoke(UDFGraphState(metadata=extracted_data))
        return result["udf_template"]
    except Exception as e:
        logger.error("Error in generate_sql_udf: %s\n%s", str(e), traceback.format_exc())
        raise
    finally:
        remove_temp_file(file_location)