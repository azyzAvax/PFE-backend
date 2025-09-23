# --- START OF FILE agent.py ---
import logging # Use logging instead of print for server apps
import os
import pandas as pd
from langgraph.graph import StateGraph, END
from utils.state import GraphState
from utils.nodes import (
    extract_and_fetch_ddls,
    generate_unit_tests,
    execute_and_verify_tests,
)
from utils.tools import get_procedure_ddl
import tempfile
import re
from langgraph.graph import StateGraph, END
from utils.pipe_state import PipeGraphState
from utils.pipe_nodes import (
    get_pipe_details,
    generate_csv_data,
    upload_and_verify_pipe,
)
import time
from typing import Dict, Any, Optional
import io

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# --- Custom Exceptions for better API error handling ---
class ProcedureNotFoundError(Exception):
    pass

class GraphExecutionError(Exception):
    pass

class ReportCreationError(Exception):
    pass

class PipeTestError(Exception): # New exception for pipe tests
    pass


def _sanitize_sheet_name(name: str) -> str:
    """Removes invalid characters and shortens sheet names for Excel."""
    # Remove invalid characters: \ / * ? [ ] :
    name = re.sub(r'[\\/*?:\[\]]', '', name)
    # Replace other potential problem characters like spaces with underscores
    name = name.replace(' ', '_')
    # Excel sheet name length limit is 31 characters
    return name[:31]

# --- Helper function to make datetimes timezone-naive ---
def _make_datetimes_naive(df: pd.DataFrame) -> pd.DataFrame:
    """Converts all timezone-aware datetime columns in a DataFrame to timezone-naive."""
    if df is None or df.empty:
        return df
    for col in df.select_dtypes(include=['datetime64[ns, UTC]', 'datetimetz']).columns:
        # Check specifically for timezone-aware types
        if df[col].dt.tz is not None:
            try:
                df[col] = df[col].dt.tz_localize(None)
                logging.debug(f"Converted column '{col}' to timezone-naive.")
            except Exception as e:
                logging.warning(f"Could not convert column '{col}' to timezone-naive: {e}")
    return df
# --- End of helper function ---


def create_excel_report(ddl_data: list, test_results: list, filename_prefix: str) -> str:
    """
    Creates an Excel file with DDL (including roles), test results summary, and detailed
    source/target data sheets for each test case in a temporary directory.
    Ensures datetimes are timezone-naive for Excel compatibility.
    Fixes sheet name collision issue.
    Returns the full path to the created file.
    """
    temp_file = None # Initialize in case of early error
    try:
        temp_file = tempfile.NamedTemporaryFile(suffix=".xlsx", prefix=filename_prefix + "_", delete=False)
        filename = temp_file.name
        temp_file.close()

        logging.info(f"Attempting to create Excel report: {filename}")
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            workbook = writer.book

            # --- DDL Data Sheet (Keep as is) ---
            if ddl_data:
                ddl_cols_order = ['objname', 'objtype', 'objrole', 'objddl']
                ddl_df = pd.DataFrame(ddl_data)
                ddl_df = ddl_df.reindex(columns=ddl_cols_order, fill_value='N/A')
                ddl_df.to_excel(writer, sheet_name='DDL Data', index=False)
                worksheet = writer.sheets['DDL Data']
                worksheet.set_column(0, 0, 40)
                worksheet.set_column(1, 1, 12)
                worksheet.set_column(2, 2, 12)
                worksheet.set_column(3, 3, 100)
                logging.info("DDL data (including roles) written to Excel.")
            else:
                logging.warning("No DDL data to write to Excel.")
                pd.DataFrame([]).to_excel(writer, sheet_name='DDL Data', index=False)

            # --- Test Results Summary Sheet ---
            if test_results:
                summary_results = []
                # Add Test Number during summary creation
                for i, result in enumerate(test_results):
                    summary = {k: v for k, v in result.items() if k not in ['source_data_before_sp', 'target_data_after_sp']}
                    summary['Test #'] = f"1-{i+1}"
                    summary_results.append(summary)

                test_results_df = pd.DataFrame(summary_results)
                
                # Rename 'test_case' column to 'Test Case Description' for clarity
                if 'test_case' in test_results_df.columns:
                    test_results_df.rename(columns={'test_case': 'Test Case Description'}, inplace=True)

                # Define the new column order including the new columns
                cols_order = [
                    'Test #', 'Test Case Description', 'insert_query', 'validation_query',
                    'expected_count', 'actual_count', 'result', 'details'
                ]
                # Reindex, filling missing columns with 'N/A'
                test_results_df = test_results_df.reindex(columns=cols_order, fill_value='N/A')
                
                test_results_df.to_excel(writer, sheet_name='Test Results Summary', index=False)
                worksheet = writer.sheets['Test Results Summary']
                
                # Adjust column widths for the new layout
                worksheet.set_column(0, 0, 10)  # Test #
                worksheet.set_column(1, 1, 40)  # Test Case Description
                worksheet.set_column(2, 2, 80)  # insert_query
                worksheet.set_column(3, 3, 60)  # validation_query
                worksheet.set_column(4, 4, 15)  # expected_count
                worksheet.set_column(5, 5, 15)  # actual_count
                worksheet.set_column(6, 6, 10)  # result
                worksheet.set_column(7, 7, 60)  # details
                logging.info("Test results summary written to Excel with Test # and Description.")


                # --- Individual Test Case Data Sheets ---
                for i, result_data in enumerate(test_results):
                    test_case_name = result_data.get('test_case', f'Test_{i+1}')
                    
                    # --- START: MODIFIED Sheet Name Generation as per user request ---
                    source_sheet_name = f"#1-{i+1}_input"
                    target_sheet_name = f"#1-{i+1}_output"
                    # --- END: MODIFIED Sheet Name Generation ---


                    # Source Data Sheet
                    source_df = result_data.get('source_data_before_sp')
                    source_df = _make_datetimes_naive(source_df)
                    # Use the correctly generated source_sheet_name
                    if source_df is not None and not source_df.empty:
                        source_df.to_excel(writer, sheet_name=source_sheet_name, index=False)
                        worksheet_source = writer.sheets[source_sheet_name]
                        for j, col in enumerate(source_df.columns):
                             worksheet_source.set_column(j, j, max(len(str(col)), 15) + 2)
                        logging.info(f"Source data for '{test_case_name}' written to sheet '{source_sheet_name}'.")
                    elif source_df is not None:
                         pd.DataFrame([{"Status": f"No source data captured or table was empty for {test_case_name}"}]).to_excel(writer, sheet_name=source_sheet_name, index=False)
                         logging.info(f"Empty source data sheet '{source_sheet_name}' created for '{test_case_name}'.")
                    else:
                         pd.DataFrame([{"Error": f"Failed to capture source data for {test_case_name}"}]).to_excel(writer, sheet_name=source_sheet_name, index=False)
                         logging.warning(f"Source data sheet '{source_sheet_name}' indicates error for '{test_case_name}'.")


                    # Target Data Sheet
                    target_df = result_data.get('target_data_after_sp')
                    target_df = _make_datetimes_naive(target_df)
                     # Use the correctly generated target_sheet_name
                    if target_df is not None and not target_df.empty:
                        target_df.to_excel(writer, sheet_name=target_sheet_name, index=False)
                        worksheet_target = writer.sheets[target_sheet_name]
                        for j, col in enumerate(target_df.columns):
                             worksheet_target.set_column(j, j, max(len(str(col)), 15) + 2)
                        logging.info(f"Target data for '{test_case_name}' written to sheet '{target_sheet_name}'.")
                    elif target_df is not None:
                         pd.DataFrame([{"Status": f"No target data captured or table was empty for {test_case_name}"}]).to_excel(writer, sheet_name=target_sheet_name, index=False)
                         logging.info(f"Empty target data sheet '{target_sheet_name}' created for '{test_case_name}'.")
                    else:
                         pd.DataFrame([{"Error": f"Failed to capture target data for {test_case_name}"}]).to_excel(writer, sheet_name=target_sheet_name, index=False)
                         logging.warning(f"Target data sheet '{target_sheet_name}' indicates error for '{test_case_name}'.")

            else:
                logging.warning("No test results to write to Excel.")
                pd.DataFrame([]).to_excel(writer, sheet_name='Test Results Summary', index=False)

            # ExcelWriter context manager handles saving on exit

        if os.path.exists(filename):
            logging.info(f"Excel report successfully created: {filename}")
            return filename
        else:
            raise ReportCreationError(f"Excel file was not found after attempting creation: {filename}")

    except Exception as e:
        logging.error(f"Failed to create Excel report '{filename_prefix}.xlsx': {e}", exc_info=True)
        if temp_file and os.path.exists(temp_file.name):
             try:
                 os.remove(temp_file.name)
                 logging.info(f"Cleaned up failed temporary report file: {temp_file.name}")
             except OSError as rm_err:
                 logging.error(f"Error removing temporary file {temp_file.name} after write failure: {rm_err}")
        raise ReportCreationError(f"Failed to create Excel report: {e}")


def create_pipe_test_excel_report(state: PipeGraphState, filename_prefix: str) -> str:
    """
    Creates an Excel report for Snowpipe test results.
    Returns the full path to the created Excel file.
    """
    temp_file = None
    try:
        temp_file = tempfile.NamedTemporaryFile(suffix=".xlsx", prefix=filename_prefix + "_pipe_test_", delete=False)
        filename = temp_file.name
        temp_file.close()

        logging.info(f"Attempting to create Pipe Test Excel report: {filename}")
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            # Sheet 1: Test Summary
            summary_data = {
                "Pipe Name": state.get("pipe_name"),
                "Pipe Schema": state.get("pipe_schema"),
                "Target Table Name": state.get("target_table_name"),
                "Azure Folder Path": state.get("azure_folder_path"),
                "Generated CSV Filename": state.get("generated_csv_filename"),
                "Upload Successful": state.get("upload_status"),
                "Verification Query (Count)": state.get("verification_query"),
                "Target Table Row Count (After Test)": state.get("verification_result"),
                "Final Message": state.get("final_message"),
                "Error Message": state.get("error_message")
            }
            summary_df = pd.DataFrame([summary_data])
            summary_df = summary_df.T.reset_index() # Transpose for better readability
            summary_df.columns = ["Field", "Value"]
            summary_df.to_excel(writer, sheet_name="Pipe Test Summary", index=False)
            worksheet_summary = writer.sheets["Pipe Test Summary"]
            worksheet_summary.set_column(0, 0, 40) # Field column width
            worksheet_summary.set_column(1, 1, 80) # Value column width
            logging.info("Pipe test summary written to Excel.")

            # Sheet 2: Generated CSV Data
            csv_content = state.get("generated_csv_content")
            csv_filename = state.get("generated_csv_filename", "generated_data.csv")
            csv_sheet_name = _sanitize_sheet_name("Generated CSV")
            if csv_content:
                try:
                    csv_df = pd.read_csv(io.StringIO(csv_content))
                    csv_df = _make_datetimes_naive(csv_df)
                    csv_df.to_excel(writer, sheet_name=csv_sheet_name, index=False)
                    worksheet_csv = writer.sheets[csv_sheet_name]
                    for j, col in enumerate(csv_df.columns):
                        worksheet_csv.set_column(j, j, max(len(str(col)), 15) + 2)
                    logging.info(f"Generated CSV data ('{csv_filename}') written to sheet '{csv_sheet_name}'.")
                except Exception as e:
                    logging.error(f"Could not parse generated CSV content for Excel: {e}")
                    pd.DataFrame([{"Error": f"Could not parse CSV: {e}", "Content": csv_content}]).to_excel(writer, sheet_name=csv_sheet_name, index=False)
            else:
                pd.DataFrame([{"Status": "No CSV data generated or available."}]).to_excel(writer, sheet_name=csv_sheet_name, index=False)
                logging.warning("No CSV data to write to Excel.")

            # Sheet 3: Target Table Data (after test)
            target_data_df = state.get("target_table_data_after_test")
            target_table_name = state.get("target_table_name", "TargetTable")
            target_data_sheet_name = _sanitize_sheet_name(f"{target_table_name}_Data")

            if target_data_df is not None and not target_data_df.empty:
                target_data_df = _make_datetimes_naive(target_data_df)
                target_data_df.to_excel(writer, sheet_name=target_data_sheet_name, index=False)
                worksheet_target_data = writer.sheets[target_data_sheet_name]
                for j, col in enumerate(target_data_df.columns):
                    worksheet_target_data.set_column(j, j, max(len(str(col)), 15) + 2)
                logging.info(f"Target table data for '{target_table_name}' written to sheet '{target_data_sheet_name}'.")
            elif target_data_df is not None: # Empty DataFrame
                pd.DataFrame([{"Status": f"Target table '{target_table_name}' was empty or no data retrieved after test."}]).to_excel(writer, sheet_name=target_data_sheet_name, index=False)
                logging.info(f"Target table '{target_table_name}' was empty, sheet '{target_data_sheet_name}' created with status.")
            else: # None
                pd.DataFrame([{"Error": f"Failed to retrieve data from target table '{target_table_name}'."}]).to_excel(writer, sheet_name=target_data_sheet_name, index=False)
                logging.warning(f"Failed to retrieve data from target table '{target_table_name}', sheet '{target_data_sheet_name}' indicates error.")
        
        if os.path.exists(filename):
            logging.info(f"Pipe Test Excel report successfully created: {filename}")
            return filename
        else:
            raise ReportCreationError(f"Pipe Test Excel file was not found after attempting creation: {filename}")

    except Exception as e:
        logging.error(f"Failed to create Pipe Test Excel report '{filename_prefix}.xlsx': {e}", exc_info=True)
        if temp_file and os.path.exists(temp_file.name):
            try:
                os.remove(temp_file.name)
                logging.info(f"Cleaned up failed temporary pipe test report file: {temp_file.name}")
            except OSError as rm_err:
                logging.error(f"Error removing temporary pipe test report file {temp_file.name} after write failure: {rm_err}")
        raise ReportCreationError(f"Failed to create Pipe Test Excel report: {e}")


# --- Graph Definition --- (Keep as is)
workflow = StateGraph(GraphState)
workflow.add_node("extract_and_fetch_ddls", extract_and_fetch_ddls)
workflow.add_node("generate_unit_tests", generate_unit_tests)
workflow.add_node("execute_and_verify_tests", execute_and_verify_tests)
workflow.add_edge("extract_and_fetch_ddls", "generate_unit_tests")
workflow.add_edge("generate_unit_tests", "execute_and_verify_tests")
workflow.add_edge("execute_and_verify_tests", END)
workflow.set_entry_point("extract_and_fetch_ddls")
graph = workflow.compile()

# --- New Pipe Test Graph Definition ---
pipe_workflow = StateGraph(PipeGraphState)

# Add nodes
pipe_workflow.add_node("get_pipe_details", get_pipe_details)
pipe_workflow.add_node("generate_csv_data", generate_csv_data)
pipe_workflow.add_node("upload_and_verify_pipe", upload_and_verify_pipe)

# Define edges
pipe_workflow.set_entry_point("get_pipe_details")
pipe_workflow.add_edge("get_pipe_details", "generate_csv_data")
pipe_workflow.add_edge("generate_csv_data", "upload_and_verify_pipe")
pipe_workflow.add_edge("upload_and_verify_pipe", END)

# Compile the graph
pipe_test_graph = pipe_workflow.compile()


def run_pipe_test_graph(pipe_name: str, pipe_schema: str) -> str:
    """
    Runs the Snowpipe test graph and creates an Excel report.
    Returns the path to the generated Excel report file.
    Raises PipeTestError or ReportCreationError on failure.
    """
    logging.info(f"Starting Snowpipe test graph for {pipe_schema}.{pipe_name}")

    initial_state = PipeGraphState(
        pipe_name=pipe_name,
        pipe_schema=pipe_schema,
        pipe_ddl=None,
        target_table_name=None,
        target_table_ddl=None,
        azure_folder_path=None,
        generated_csv_content=None,
        generated_csv_filename=None,
        upload_status=None,
        verification_query=None,
        verification_result=None,
        target_table_data_after_test=None, # Initialize
        final_message="Graph execution started.",
        error_message=None,
        messages=[],
    )

    final_state: Optional[PipeGraphState] = None
    try:
        final_state = pipe_test_graph.invoke(initial_state)
        logging.info(f"Pipe test graph execution finished for {pipe_schema}.{pipe_name}")

    except Exception as e:
        logging.error(f"Pipe test graph invocation error for {pipe_schema}.{pipe_name}: {e}", exc_info=True)
        raise PipeTestError(f"An error occurred during pipe test graph execution: {e}")

    if not final_state:
        raise PipeTestError("Pipe test graph execution did not return a final state.")

    # Check for critical errors in the final state from graph execution
    # e.g. pipe not found, DDL extraction failed early.
    if final_state.get("error_message") and ("not found" in final_state["error_message"].lower() or "failed to get ddl" in final_state["error_message"].lower()):
        logging.error(f"Critical error in pipe test for {pipe_schema}.{pipe_name}: {final_state['error_message']}")
        # This error will be included in the report, but we might also want to raise it here
        # if it prevents report generation or makes the report meaningless.
        # For now, let the report be generated with the error message.

    try:
        filename_prefix = f"{pipe_schema}_{pipe_name}"
        report_filename = create_pipe_test_excel_report(final_state, filename_prefix)
        return report_filename
    except ReportCreationError as e:
        # Logged inside create_pipe_test_excel_report
        raise # Re-raise
    except Exception as e:
        logging.error(f"Unexpected error during pipe test report creation: {e}", exc_info=True)
        raise ReportCreationError(f"Unexpected error during pipe test report creation: {e}")


# --- run_graph_for_api --- (Keep as is, except maybe update state type hint if strict)
def run_graph_for_api(procedure_name: str, procedure_schema: str) -> str:
    """
    Runs the DDL extraction and unit test generation, creates a report.
    Returns the path to the generated Excel report file.
    Raises exceptions on failure.
    """
    logging.info(f"Starting graph for {procedure_schema}.{procedure_name}")
    initial_procedure_ddl = get_procedure_ddl(procedure_name, procedure_schema)

    if not initial_procedure_ddl or "Error" in initial_procedure_ddl or "not found" in initial_procedure_ddl:
        error_msg = f"Error getting initial DDL for {procedure_schema}.{procedure_name}: {initial_procedure_ddl}"
        logging.error(error_msg)
        raise ProcedureNotFoundError(error_msg) # Raise specific error

    inputs = {
        "procedure_name": procedure_name,
        "procedure_schema": procedure_schema,
        "procedure_ddl": initial_procedure_ddl,
        # ddl_data now implicitly includes 'objrole' from the node
        "ddl_data": [],
        "messages": [],
        "unit_tests": [],
        "test_results": [],
        "truncated_tables": set(), # Initialize the set for truncated tables
    }

    final_state = None
    try:
        # Make sure to handle potential long runs if necessary (e.g., background tasks)
        # For now, run synchronously
        final_state = graph.invoke(inputs)
        logging.info(f"Graph execution finished for {procedure_schema}.{procedure_name}")
        # logging.debug(f"Final State: {final_state}") # Log full state only if needed

    except Exception as e:
        logging.error(f"Graph invocation error for {procedure_schema}.{procedure_name}: {e}", exc_info=True)
        # Optionally inspect intermediate state if needed for debugging
        raise GraphExecutionError(f"An error occurred during graph execution: {e}")

    if not final_state:
         raise GraphExecutionError("Graph execution did not return a final state.")

    # ddl_data fetched from state will now have the 'objrole' key
    ddl_data = final_state.get('ddl_data', [])
    # test_results now contains dictionaries with DataFrames
    test_results = final_state.get('test_results', [])
    # logging.info(final_state) # Careful, this state can be large now

    # Create the report using the modified function
    try:
        # Generate a safe filename prefix
        filename_prefix = f"{procedure_schema}_{procedure_name}_report"
        report_filename = create_excel_report(ddl_data, test_results, filename_prefix)
        return report_filename # Return the full path
    except ReportCreationError as e:
         # Logged inside create_excel_report
         raise # Re-raise the specific error
    except Exception as e: # Catch other unexpected errors during run_graph_for_api
        logging.error(f"Unexpected error during graph execution or report preparation: {e}", exc_info=True)
        raise GraphExecutionError(f"Unexpected error during process: {e}")