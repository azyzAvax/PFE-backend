from urllib import request
import shutil
from fastapi import APIRouter, UploadFile, File, Form, Depends, Body,BackgroundTasks,HTTPException
from fastapi.responses import JSONResponse,FileResponse
from typing import List, Dict,Optional
from services.ddl_service import generate_ddl, get_sheet_names, generate_ddl_from_sheets, generate_ddl_from_design
from services.usp_service import generate_usp
from services.pipe_service import generate_pipe, generate_pipe_with_json
from services.udf_service import generate_js_udf, generate_sql_udf
from services.project_service import initialize_project, list_active_projects
from utils.env_utils import get_env_vars
from utils.azure_utils import get_auth_headers, generate_changelog_sql
import logging
import os
import json
from utils.azure_utils import validate_env_vars
import requests
router = APIRouter()
logger = logging.getLogger("uvicorn.error")
AZURE_PAT = os.getenv("AZURE_PAT")
AZURE_ORG_URL = os.getenv("AZURE_ORG_URL")
AZURE_PROJECT = os.getenv("AZURE_PROJECT")
AZURE_REPO_ID = os.getenv("AZURE_REPO_ID")

from pydantic import BaseModel,Field

    
from services.dashboard_pipeline_service import DashboardPipelineService

from fastapi import APIRouter, Request
from typing import Dict, Any
class DashboardRequest(BaseModel):
    dashboard_prompt: str




@router.post("/api/pipeline/dashboard")
async def generate_dashboard_pipeline(req: Request, request_body: DashboardRequest):
    """
    Generate a complete DataOps pipeline for a dashboard request.

    - Step 1: Extract KPIs, dimensions, and data transformation flow.
    - Step 2: Generate a visual flow (Mermaid diagram).
    - Step 3: Provide natural language explanations of each step.
    """
    try:
        
        service = DashboardPipelineService()
        result = service.generate_pipeline(request_body.dict())

        if not result.get("success"):
            return JSONResponse(
                status_code=500,
                content={
                    "error": result.get("error", "Unknown error"),
                    "success": False
                },
            )

        return JSONResponse(
            content={
                "success": True,
                "flow": result["flow"],
                "diagram": result["diagram"],
                "explanations": result["explanations"],
            },
            status_code=200,
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": str(e),
            },
        )

@router.post("/generate-ddl/")
async def generate_ddl_endpoint(file: UploadFile = File(...), env_vars: dict = Depends(get_env_vars)):
    """Generate DDL from an Excel file."""
    try:
        result = await generate_ddl(file, env_vars)
        return JSONResponse({"ddl": result})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/get-sheet-names/")
async def get_sheet_names_endpoint(file: UploadFile = File(...), env_vars: dict = Depends(get_env_vars)):
    """Retrieve sheet names from an Excel file."""
    try:
        sheet_names = await get_sheet_names(file, env_vars)
        return {"sheet_names": sheet_names}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/generate-ddl-from-sheets/")
async def generate_ddl_from_sheets_endpoint(file: UploadFile = File(...), sheets: str = Form(...), env_vars: dict = Depends(get_env_vars)):
    """Generate DDLs for multiple sheets."""
    try:
        result = await generate_ddl_from_sheets(file, sheets, env_vars)
        return {"ddls": result}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/generate-ddl-from-design/")
async def generate_ddl_from_design_endpoint(file: UploadFile = File(...), sheets: str = Form(...), env_vars: dict = Depends(get_env_vars)):
    """Generate DDLs from design sheets using LLM."""
    try:
        result = await generate_ddl_from_design(file, sheets, env_vars)
        return {"ddls": result}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/generate-usp-from-sheet/")
async def generate_usp_endpoint(file: UploadFile = File(...), sheet_name: str = Form(...), env_vars: dict = Depends(get_env_vars)):
    """Generate Snowflake USP from an Excel sheet."""
    try:
        result = await generate_usp(file, sheet_name, env_vars)
        if not result or "usp_template" not in result:
            return JSONResponse({"error": "USP generation failed or returned incomplete result."}, status_code=500)

        return {
            "metadata": result.get("metadata", {}),
            "usp_template": result.get("usp_template", "")
        }
    except Exception as e:
        logger.error(f"Exception in USP endpoint: {str(e)}")
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/generate-pipe/")
async def generate_pipe_endpoint(file: UploadFile = File(...), sheet_name: str = Form(...), env_vars: dict = Depends(get_env_vars)):
    """Generate Snowflake PIPE from an Excel sheet."""
    try:
        result = await generate_pipe(file, sheet_name, env_vars)
        return {"pipe": result}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/generate-pipe-with-json/")
async def generate_pipe_with_json_endpoint(json_data: dict = Body(...), env_vars: dict = Depends(get_env_vars)):
    """Generate Snowflake PIPE from JSON data."""
    try:
        result = await generate_pipe_with_json(json_data, env_vars)
        return {"pipe": result}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/generate-js-udf-from-sheet/")
async def generate_js_udf_endpoint(file: UploadFile = File(...), sheet_name: str = Form(...), env_vars: dict = Depends(get_env_vars)):
    """Generate Snowflake JavaScript UDF from an Excel sheet."""
    try:
        result = await generate_js_udf(file, sheet_name, env_vars)
        return {"udf_template": result}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/generate-sql-udf-from-sheet/")
async def generate_sql_udf_endpoint(file: UploadFile = File(...), sheet_name: str = Form(...), env_vars: dict = Depends(get_env_vars)):
    """Generate Snowflake SQL UDF from an Excel sheet."""
    try:
        result = await generate_sql_udf(file, sheet_name, env_vars)
        return {"udf_template": result}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/deploy-to-azure/")
async def deploy_to_azure_endpoint(file: UploadFile = File(...), branch_name: str = Form(...), env_vars: dict = Depends(get_env_vars)):
    """Deploy a single SQL file to Azure DevOps."""
    try:
        file_content = (await file.read()).decode()
        filename = file.filename

        os.makedirs("temp", exist_ok=True)
        sql_path = os.path.join("temp", filename)
        with open(sql_path, "w") as f:
            f.write(file_content)

        changelog_path = os.path.join("temp", f"changelog_{filename.replace('.sql', '.yml')}")
        generate_changelog_sql([sql_path], changelog_path)

        with open(changelog_path, "r") as f:
            changelog_content = f.read()

        refs_url = f"{env_vars['AZURE_ORG_URL']}/{env_vars['AZURE_PROJECT']}/_apis/git/repositories/{env_vars['AZURE_REPO_ID']}/refs?filter=heads/main&api-version=7.1-preview.1"
        refs_res = json.loads(request.urlopen(request.Request(refs_url, headers=get_auth_headers(env_vars.get('AZURE_PAT')))).read().decode())
        main_commit = refs_res["value"][0]["objectId"]

        create_branch_url = f"{env_vars['AZURE_ORG_URL']}/{env_vars['AZURE_PROJECT']}/_apis/git/repositories/{env_vars['AZURE_REPO_ID']}/refs?api-version=7.1-preview.1"
        branch_data = json.dumps([{
            "name": f"refs/heads/{branch_name}",
            "oldObjectId": "0000000000000000000000000000000000000000",
            "newObjectId": main_commit
        }]).encode()
        request.urlopen(request.Request(create_branch_url, data=branch_data, headers=get_auth_headers(env_vars.get('AZURE_PAT')), method="POST"))

        commit_url = f"{env_vars['AZURE_ORG_URL']}/{env_vars['AZURE_PROJECT']}/_apis/git/repositories/{env_vars['AZURE_REPO_ID']}/pushes?api-version=7.1-preview.2"
        push_data = json.dumps({
            "refUpdates": [{
                "name": f"refs/heads/{branch_name}",
                "oldObjectId": main_commit
            }],
            "commits": [{
                "comment": f"Add {filename} and changelog",
                "changes": [
                    {
                        "changeType": "add",
                        "item": {"path": f"/usp/{filename}"},
                        "newContent": {
                            "content": file_content,
                            "contentType": "rawtext"
                        }
                    },
                    {
                        "changeType": "add",
                        "item": {"path": f"/changelogs/{os.path.basename(changelog_path)}"},
                        "newContent": {
                            "content": changelog_content,
                            "contentType": "rawtext"
                        }
                    }
                ]
            }]
        }).encode()
        request.urlopen(request.Request(commit_url, data=push_data, headers=get_auth_headers(env_vars.get('AZURE_PAT')), method="POST"))

        pr_url = f"{env_vars['AZURE_ORG_URL']}/{env_vars['AZURE_PROJECT']}/_apis/git/repositories/{env_vars['AZURE_REPO_ID']}/pullrequests?api-version=7.1-preview.1"
        pr_data = json.dumps({
            "sourceRefName": f"refs/heads/{branch_name}",
            "targetRefName": "refs/heads/main",
            "title": f"Merge {branch_name} into main",
            "description": f"Generated USP file {filename} and changelog"
        }).encode()
        pr_res = json.loads(request.urlopen(request.Request(pr_url, data=pr_data, headers=get_auth_headers(env_vars.get('AZURE_PAT')), method="POST")).read().decode())

        return {
            "branch": branch_name,
            "file_uploaded": filename,
            "changelog_uploaded": os.path.basename(changelog_path),
            "pull_requests_url": pr_res.get("url")
        }
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        for temp_file in [sql_path, changelog_path]:
            if os.path.exists(temp_file):
                os.remove(temp_file)

@router.post("/deploy-multiple-to-azure/")
async def deploy_multiple_to_azure_endpoint(files: List[UploadFile] = File(...), branch_name: str = Form(...), description: str = Form(""), env_vars: dict = Depends(get_env_vars)):
    """Deploy multiple SQL files to Azure DevOps."""
    try:
        os.makedirs("temp", exist_ok=True)
        
        file_contents = []
        filenames = []
        sql_paths = []
        
        for file in files:
            file_content = (await file.read()).decode()
            filename = file.filename
            filenames.append(filename)
            
            sql_path = os.path.join("temp", filename)
            with open(sql_path, "w") as f:
                f.write(file_content)
            sql_paths.append(sql_path)
                
            file_contents.append({
                "filename": filename,
                "content": file_content
            })
        
        changelog_path = os.path.join("temp", f"changelog_batch_{branch_name}.yml")
        generate_changelog_sql(sql_paths, changelog_path)
        
        with open(changelog_path, "r") as f:
            changelog_content = f.read()
        
        refs_url = f"{env_vars['AZURE_ORG_URL']}/{env_vars['AZURE_PROJECT']}/_apis/git/repositories/{env_vars['AZURE_REPO_ID']}/refs?filter=heads/main&api-version=7.1-preview.1"
        refs_res = json.loads(request.urlopen(request.Request(refs_url, headers=get_auth_headers(env_vars.get('AZURE_PAT')))).read().decode())
        main_commit = refs_res["value"][0]["objectId"]
        
        create_branch_url = f"{env_vars['AZURE_ORG_URL']}/{env_vars['AZURE_PROJECT']}/_apis/git/repositories/{env_vars['AZURE_REPO_ID']}/refs?api-version=7.1-preview.1"
        branch_data = json.dumps([{
            "name": f"refs/heads/{branch_name}",
            "oldObjectId": "0000000000000000000000000000000000000000",
            "newObjectId": main_commit
        }]).encode()
        request.urlopen(request.Request(create_branch_url, data=branch_data, headers=get_auth_headers(env_vars.get('AZURE_PAT')), method="POST"))
        
        changes = []
        
        for file_info in file_contents:
            changes.append({
                "changeType": "add",
                "item": {"path": f"/usp/{file_info['filename']}"},
                "newContent": {
                    "content": file_info['content'],
                    "contentType": "rawtext"
                }
            })
        
        changes.append({
            "changeType": "add",
            "item": {"path": f"/changelogs/{os.path.basename(changelog_path)}"},
            "newContent": {
                "content": changelog_content,
                "contentType": "rawtext"
            }
        })
        
        commit_url = f"{env_vars['AZURE_ORG_URL']}/{env_vars['AZURE_PROJECT']}/_apis/git/repositories/{env_vars['AZURE_REPO_ID']}/pushes?api-version=7.1-preview.2"
        push_data = json.dumps({
            "refUpdates": [{
                "name": f"refs/heads/{branch_name}",
                "oldObjectId": main_commit
            }],
            "commits": [{
                "comment": f"Add {len(filenames)} files and changelog",
                "changes": changes
            }]
        }).encode()
        request.urlopen(request.Request(commit_url, data=push_data, headers=get_auth_headers(env_vars.get('AZURE_PAT')), method="POST"))
        
        pr_url = f"{env_vars['AZURE_ORG_URL']}/{env_vars['AZURE_PROJECT']}/_apis/git/repositories/{env_vars['AZURE_REPO_ID']}/pullrequests?api-version=7.1-preview.1"
        pr_description = description if description else f"Generated {len(filenames)} files and changelog"
        pr_data = json.dumps({
            "sourceRefName": f"refs/heads/{branch_name}",
            "targetRefName": "refs/heads/main",
            "title": f"Merge {branch_name} into main",
            "description": pr_description
        }).encode()
        pr_res = json.loads(request.urlopen(request.Request(pr_url, data=pr_data, headers=get_auth_headers(env_vars.get('AZURE_PAT')), method="POST")).read().decode())
        
        return {
            "branch": branch_name,
            "files_uploaded": filenames,
            "changelog_uploaded": os.path.basename(changelog_path),
            "pull_requests_url": pr_res.get("url")
        }
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        for temp_file in sql_paths + [changelog_path]:
            if os.path.exists(temp_file):
                os.remove(temp_file)

@router.post("/initialize-project/")
async def initialize_project_endpoint(project: dict = Body(...), env_vars: dict = Depends(get_env_vars)):
    """Initialize a new project with a name, description, and type."""
    try:
        result = await initialize_project(project, env_vars)
        return result
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/list-active-projects/")
async def list_active_projects_endpoint(env_vars: dict = Depends(get_env_vars)):
    """List all active projects in Azure DevOps."""
    try:
        result = await list_active_projects(env_vars)
        return result
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    


class ProcedureInput(BaseModel):
    procedure_name: str = Field(..., description="The name of the Snowflake stored procedure.")
    procedure_schema: str = Field(..., description="The schema of the Snowflake stored procedure.")
class PipeInput(BaseModel): # New model for Pipe testing
    pipe_name: str = Field(..., description="The name of the Snowflake Snowpipe.")
    pipe_schema: str = Field(..., description="The schema of the Snowflake Snowpipe.")

# --- Helper function for cleanup ---


from models.agent import (
    run_graph_for_api,
    ProcedureNotFoundError,
    GraphExecutionError,
    ReportCreationError,
    run_pipe_test_graph,
    PipeTestError # Import new exception
)

class ProcedureInput(BaseModel):
    procedure_name: str = Field(..., description="The name of the Snowflake stored procedure.")
    procedure_schema: str = Field(..., description="The schema of the Snowflake stored procedure.")
class PipeInput(BaseModel): # New model for Pipe testing
    pipe_name: str = Field(..., description="The name of the Snowflake Snowpipe.")
    pipe_schema: str = Field(..., description="The schema of the Snowflake Snowpipe.")

def remove_file(path: str) -> None:
    """Removes a file, logging errors if any."""
    try:
        os.remove(path)
        logging.info(f"Successfully removed temporary file: {path}")
    except OSError as e:
        logging.error(f"Error removing temporary file {path}: {e}", exc_info=True)

# --- API Endpoint ---
@router.post(
    "/generate-test-report",
    response_class=FileResponse, # Expecting to return a file
    summary="Generate Unit Test Report for Snowflake Procedure",
    description="Takes a procedure name and schema, runs the test generation process, and returns an Excel report.",
    responses={
        200: {
            "description": "Excel report generated successfully.",
            "content": {
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {
                    "schema": {
                        "type": "string",
                        "format": "binary"
                    }
                }
            }
        },
        404: {"description": "Procedure not found in Snowflake."},
        500: {"description": "Internal server error during graph execution or report generation."},
    }
)
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

# --- API Endpoint: Test Snowpipe ---
@router.post(
    "/test-snowpipe",
    response_class=FileResponse, # Expecting to return a file for success
    summary="Test Snowpipe and Generate Excel Report",
    description="Takes a pipe name and schema, generates sample CSV, uploads it, waits, verifies data loading, and returns an Excel report with test details.",
    responses={
        200: {
            "description": "Pipe test Excel report generated successfully.",
            "content": {
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {
                    "schema": {
                        "type": "string",
                        "format": "binary"
                    }
                }
            }
        },
        404: {"description": "Pipe or associated resources not found in Snowflake."},
        500: {"description": "Internal server error during pipe test execution or report generation."},
    }
)
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
    

def load_file_contents(project_env: str) -> List[Dict]:
    """Load accumulated file contents from a persistent JSON file."""
    metadata_path = os.path.join("temp", project_env, "file_metadata.json")
    if os.path.exists(metadata_path):
        with open(metadata_path, "r") as f:
            return json.load(f)
    return []

def save_file_contents(project_env: str, file_contents: List[Dict]):
    """Save accumulated file contents to a persistent JSON file."""
    metadata_path = os.path.join("temp", project_env, "file_metadata.json")
    os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
    with open(metadata_path, "w") as f:
        json.dump(file_contents, f)


@router.post("/deploy-multiple-to-azure-with-workitem/")
async def deploy_multiple_to_azure_with_workitem(
    files: List[UploadFile] = File(...), 
    branch_name: str = Form(...), 
    description: str = Form(""),
    work_item_id: Optional[int] = Form(None),
    author: str = Form(...),
    #context_dir: str = Form(...),
    context_filter: str = Form(...),
    project_env: str = Form(...),
    project_name: str = Form(...),
    stage: str = Form(...),
    env_vars: dict = Depends(get_env_vars)
):
    """
    Deploy multiple files to Azure DevOps and optionally link to a work item with corresponding changelogs.
    debug_mode enables local testing without real Azure credentials.
    """

    project_env = project_env or env_vars.get('PROJECT_ENV')
    project_name = project_name or env_vars.get('PROJECT_NAME')
    if not all([project_env, project_name, author, context_filter]):
        raise ValueError("project_env, project_name, author, and context_filter must be provided")

    temp_dir = os.path.join("temp", project_env)
    os.makedirs(temp_dir, exist_ok=True)
    
    # Optional: Clean up non-rfz directories under test_project
    project_base_dir = os.path.join(temp_dir, "snowflake", project_name)
    for context_dir in ["ddl", "dcl"]:
        base_path = os.path.join(project_base_dir, context_dir)
        if os.path.exists(base_path):
            for subdir in os.listdir(base_path):
                if subdir != "rfz" and os.path.isdir(os.path.join(base_path, subdir)):
                    shutil.rmtree(os.path.join(base_path, subdir), ignore_errors=True)
                    logger.info(f"Removed {context_dir}/{subdir} under {project_name}")
    
    # Load accumulated file contents from persistent storage
    file_contents = load_file_contents(project_env)
    existing_filenames = {f["filename"] for f in file_contents}
    filenames = []
    sql_dirs = {}
    # Mapping
    filename_mapping = {
        "dl_": {"object_type": "table", "prefix": "20"},
        "dw_t": {"object_type": "table", "prefix": "20"},
        "dw_v": {"object_type": "view", "prefix": "50"},
        "od_": {"object_type": "table", "prefix": "20"},
        "pip_dl": {"object_type": "snowpipe", "prefix": "60"},
        "stg_dl_": {"object_type": "stage", "prefix": "00"},
        "str_dl_": {"object_type": "stream", "prefix": "30"},
        "str_od": {"object_type": "stream", "prefix": "30"},
        "tsk_dl": {"object_type": "task", "prefix": "80"},
        "tsk_od": {"object_type": "task", "prefix": "80"},
        "usp_dl_t": {"object_type": "procedure", "prefix": "70"},
        "usp_od_t": {"object_type": "procedure", "prefix": "70"},
        "fmt_": {"object_type": "fileformat", "prefix": "10"}
    }
    
    new_file_contents = []
    for file in files:
        file_content = (await file.read()).decode()
        filename = file.filename
        filenames.append(filename)
        logger.info(f"Processing file: {filename}")
        
        filename_base = filename.replace(".sql", "").lower()
        matched = False
        context = "dlz"  # Default context, will be overridden by filename rules
        for prefix, mapping in filename_mapping.items():
            if filename_base.startswith(prefix):
                object_type = mapping["object_type"]
                prefix_value = mapping["prefix"]
                matched = True
                logger.info(f"Matched prefix {prefix} for {filename}, object_type: {object_type}, prefix: {prefix_value}")
                # Adjust context based on filename rules, override for fileformat
                if "_dl_" in filename_base or prefix.startswith("dl") or prefix.endswith("dl"):
                    context = "dlz"
                elif "_od_" in filename_base or prefix.startswith("od") or prefix.endswith("od"):
                    context = "trz"
                elif "_dw_" in filename_base or prefix.startswith("dw") or prefix.endswith("dw"):
                    context = "rfz"
                # Force fileformat to dlz
                if object_type == "fileformat" or object_type == "stage":
                    context = "dlz"
                break
            
        if not matched:
            logger.warning(f"Unknown filename prefix for {filename}, skipping")
            continue
        
        logger.info(f"Inferred object_type for {filename}: {object_type}, context: {context}")
        
        # Place DDL files in predefined locations
        ddl_base_dir = os.path.join(temp_dir, "snowflake", "dataplatform", "ddl", context)
        ddl_prefix_dir = os.path.join(ddl_base_dir, f"{prefix_value}_{object_type}")
        os.makedirs(ddl_prefix_dir, exist_ok=True)
        ddl_path = os.path.join(ddl_prefix_dir, filename)
        with open(ddl_path, "w", encoding="utf-8") as f:
            f.write(file_content)
        
        # Place DDL files in project-specific location only if context is rfz
        project_ddl_path = ""
        if context == "rfz":
            project_ddl_dir = os.path.join(temp_dir, "snowflake", project_name, "ddl", "rfz", f"{prefix_value}_{object_type}")
            os.makedirs(project_ddl_dir, exist_ok=True)
            project_ddl_path = os.path.join(project_ddl_dir, filename)
            with open(project_ddl_path, "w", encoding="utf-8") as f:
                f.write(file_content)
        
        # Generate and place DCL files only for tables, stages, and views
        dcl_path = ""
        project_dcl_path = ""
        if object_type in ["table", "stage", "view"]:
            dcl_filename = f"{filename.replace('.sql', '')}.sql"
            dcl_base_dir = os.path.join(temp_dir, "snowflake", "dataplatform", "dcl", context, f"{prefix_value}_{object_type}")
            os.makedirs(dcl_base_dir, exist_ok=True)
            dcl_path = os.path.join(dcl_base_dir, dcl_filename)

            role_rw = f"{project_name.upper()}_PROD_RW"
            role_ro = f"{project_name.upper()}_PROD_RO"
            object_name = filename.replace('.sql', '')
            dcl_content = ""
            if object_type == "table":
                dcl_content = f"GRANT ALL ON TABLE {context}.{object_name} TO ROLE {role_rw};\n"
                dcl_content += f"GRANT SELECT ON TABLE {context}.{object_name} TO ROLE {role_ro};\n"
            elif object_type == "view":
                dcl_content = f"GRANT ALL ON TABLE {context}.{object_name} TO ROLE {role_rw};\n"
                dcl_content += f"GRANT SELECT ON TABLE {context}.{object_name} TO ROLE {role_ro};\n"
            elif object_type == "stage":
                stage_name = f"{context}.{object_name}"
                dcl_content += f"GRANT USAGE ON STAGE {stage_name} TO ROLE PROD_ADMIN;\n"
            if dcl_content:  # Write DCL file only if content exists
                with open(dcl_path, "w", encoding="utf-8") as f:
                    f.write(dcl_content)
            
            # Place DCL files in project-specific location only if context is rfz
            if context == "rfz":
                project_dcl_dir = os.path.join(temp_dir, "snowflake", project_name, "dcl", "rfz", f"{prefix_value}_{object_type}")
                os.makedirs(project_dcl_dir, exist_ok=True)
                project_dcl_path = os.path.join(project_dcl_dir, dcl_filename)
                with open(project_dcl_path, "w", encoding="utf-8") as f:
                    f.write(dcl_content)
        
        # Append to file_contents only if new
        file_info = {
            "filename": filename,
            "content": file_content,
            "object_type": object_type,
            "context": context,
            "ddl_path": ddl_path,
            "project_ddl_path": project_ddl_path,
            "dcl_path": dcl_path if object_type in ["table", "stage", "view"] else "",
            "project_dcl_path": project_dcl_path if object_type in ["table", "stage", "view"] else "",
            "prefix": prefix_value
        }
        if filename not in existing_filenames:
            file_contents.append(file_info)
            new_file_contents.append(file_info)
        logger.info(f"Added file_info to file_contents: {file_info}")
    
    # Save updated file contents to persistent storage
    save_file_contents(project_env, file_contents)
    
    changelog_paths = {}
    # Group files by object type for batch processing
    files_by_type = {}
    for file_info in new_file_contents:
        obj_type = file_info["object_type"]
        if obj_type not in files_by_type:
            files_by_type[obj_type] = []
        files_by_type[obj_type].append(file_info["ddl_path"])
    
    # Generate changelogs for each object type
    for obj_type, sql_files in files_by_type.items():
        # Use the prefix from file_info for the changelog filename
        prefix = next((file_info["prefix"] for file_info in new_file_contents if file_info["object_type"] == obj_type), "00")
        changelog_path = os.path.join(temp_dir, "liquibase", project_name, "changelog", "01_main", f"{prefix}_{obj_type}.yaml")
        logger.info(f"Generating changelog for {obj_type} with prefix {prefix} and sql_files: {sql_files}")
        try:
            if sql_files and any(sql_files):  # Check if there are non-empty paths
                generate_changelog_sql(sql_files, changelog_path, obj_type, author, context_filter, project_env, project_name)
                changelog_paths[changelog_path] = f"01_main/{prefix}_{obj_type}.yaml"
        except Exception as e:
            logger.error(f"Failed to generate changelog for {obj_type}: {str(e)}")
            continue
    
    changelog_contents = {path: open(path, "r", encoding="utf-8").read() for path in changelog_paths.keys() if os.path.exists(path)}
    
    refs_url = f"{env_vars['AZURE_ORG_URL']}/{env_vars['AZURE_PROJECT']}/_apis/git/repositories/{env_vars['AZURE_REPO_ID']}/refs?filter=heads/main&api-version=7.1-preview.1"
    refs_res = json.loads(request.urlopen(request.Request(refs_url, headers=get_auth_headers(env_vars.get('AZURE_PAT')))).read().decode())
    main_commit = refs_res["value"][0]["objectId"]
    
    create_branch_url = f"{env_vars['AZURE_ORG_URL']}/{env_vars['AZURE_PROJECT']}/_apis/git/repositories/{env_vars['AZURE_REPO_ID']}/refs?api-version=7.1-preview.1"
    branch_data = json.dumps([{
        "name": f"refs/heads/{branch_name}",
        "oldObjectId": "0000000000000000000000000000000000000000",
        "newObjectId": main_commit
    }]).encode()
    request.urlopen(request.Request(create_branch_url, data=branch_data, headers=get_auth_headers(env_vars.get('AZURE_PAT')), method="POST"))
    
    changes = []
    for file_info in file_contents:
        prefix = file_info.get("prefix", "00")
        if file_info.get("ddl_path"):
            changes.append({
                "changeType": "add",
                "item": {"path": f"{project_env}/snowflake/dataplatform/ddl/{file_info['context']}/{prefix}_{file_info['object_type']}/{file_info['filename']}"},
                "newContent": {
                    "content": file_info['content'],
                    "contentType": "rawtext"
                }
            })
            if file_info["context"] == "rfz":
                changes.append({
                    "changeType": "add",
                    "item": {"path": f"{project_env}/snowflake/{project_name}/ddl/rfz/{prefix}_{file_info['object_type']}/{file_info['filename']}"},
                    "newContent": {
                        "content": file_info['content'],
                        "contentType": "rawtext"
                    }
                })
        # Add DCL files only for tables, stages, and views
        if file_info.get("dcl_path") and file_info["object_type"] in ["table", "stage", "view"]:
            dcl_filename = f"{file_info['filename'].replace('.sql', '')}.sql"
            changes.append({
                "changeType": "add",
                "item": {"path": f"{project_env}/snowflake/dataplatform/dcl/{file_info['context']}/{prefix}_{file_info['object_type']}/{dcl_filename}"},
                "newContent": {
                    "content": open(file_info['dcl_path'], "r", encoding="utf-8").read(),
                    "contentType": "rawtext"
                }
            })
            if file_info["context"] == "rfz":
                changes.append({
                    "changeType": "add",
                    "item": {"path": f"{project_env}/snowflake/{project_name}/dcl/rfz/{prefix}_{file_info['object_type']}/{dcl_filename}"},
                    "newContent": {
                        "content": open(file_info['project_dcl_path'], "r", encoding="utf-8").read(),
                        "contentType": "rawtext"
                    }
                })
        
        for temp_path, changelog_name in changelog_paths.items():
            changes.append({
                "changeType": "add",
                "item": {"path": f"{project_env}/liquibase/{project_name}/{changelog_name}"},
                "newContent": {
                    "content": changelog_contents.get(temp_path, ""),
                    "contentType": "rawtext"
                }
            })
        
        commit_url = f"{env_vars['AZURE_ORG_URL']}/{env_vars['AZURE_PROJECT']}/_apis/git/repositories/{env_vars['AZURE_REPO_ID']}/pushes?api-version=7.1-preview.2"
        push_data = json.dumps({
            "refUpdates": [{"name": f"refs/heads/{branch_name}", "oldObjectId": main_commit}],
            "commits": [{"comment": f"Add {len(filenames)} files and changelogs", "changes": changes}]
        }).encode()
        request.urlopen(request.Request(commit_url, data=push_data, headers=get_auth_headers(env_vars.get('AZURE_PAT')), method="POST"))
        
        pr_url = f"{env_vars['AZURE_ORG_URL']}/{env_vars['AZURE_PROJECT']}/_apis/git/repositories/{env_vars['AZURE_REPO_ID']}/pullrequests?api-version=7.1-preview.1"
        pr_description = description if description else f"Generated {len(filenames)} files and changelogs"
        pr_data = json.dumps({
            "sourceRefName": f"refs/heads/{branch_name}",
            "targetRefName": "refs/heads/main",
            "title": f"Merge {branch_name} into main",
            "description": pr_description
        }).encode()
        pr_res = json.loads(request.urlopen(request.Request(pr_url, data=pr_data, headers=get_auth_headers(env_vars.get('AZURE_PAT')), method="POST")).read().decode())
        
        temp_files = [os.path.join(sql_dirs.get(file.get("object_type", ""), ""), file.get("filename", "")) for file in file_contents]
        temp_files.extend(changelog_paths.keys())
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        return {
            "branch": branch_name,
            "files_uploaded": filenames,
            "changelogs_uploaded": list(changelog_paths.values()),
            "pull_request_url": pr_res.get("url"),
            "work_item_linked": work_item_id is not None,
            "work_item_id": work_item_id
        }

@router.post("/initialize-project/")
async def initialize_project_endpoint(project: dict = Body(...), env_vars: dict = Depends(get_env_vars)):
    """Initialize a new project with a name, description, and type."""
    try:
        result = await initialize_project(project, env_vars)
        return result
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/list-active-projects/")
async def list_active_projects_endpoint(env_vars: dict = Depends(get_env_vars)):
    """List all active projects in Azure DevOps."""
    try:
        result = await list_active_projects(env_vars)
        return result
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    

@router.get("/search-work-items/")
async def search_work_items(query: str = "", limit: int = 20):
    """
    Search work items across all projects in Azure DevOps organization by ID or title.
    """
    logger.info(f"search_work_items called with query: '{query}'")
    
    # Check if Azure configuration is available
    if not AZURE_PAT or not AZURE_ORG_URL:
        logger.error("Azure configuration missing")
        return JSONResponse({"error": "Azure DevOps configuration is missing"}, status_code=500)
    
    try:
        if not query.strip():
            return {"work_items": []}
            
        # Check if query is a number (work item ID)
        is_id_search = query.strip().isdigit()
        
        if is_id_search:
            # Direct work item lookup by ID
            work_item_id = int(query.strip())
            work_items_url = f"{AZURE_ORG_URL}/_apis/wit/workitems/{work_item_id}?api-version=7.1-preview.3"
            work_items_response = requests.get(work_items_url, headers=get_auth_headers(AZURE_PAT))
            
            if work_items_response.status_code == 404:
                return {"work_items": []}
            elif work_items_response.status_code >= 400:
                return JSONResponse({"error": f"Failed to get work item: {work_items_response.text}"}, status_code=500)
            
            work_item = work_items_response.json()
            fields = work_item.get("fields", {})
            
            formatted_work_item = {
                "id": work_item["id"],
                "title": fields.get("System.Title", ""),
                "state": fields.get("System.State", ""),
                "type": fields.get("System.WorkItemType", ""),
                "project": fields.get("System.TeamProject", ""),
                "assignedTo": fields.get("System.AssignedTo", {}).get("displayName", "Unassigned") if fields.get("System.AssignedTo") else "Unassigned",
                "url": work_item.get("_links", {}).get("html", {}).get("href", "")
            }
            
            return {"work_items": [formatted_work_item]}
        
        else:
            # Search by title across all projects
            wiql_query = {
                "query": f"""
                    SELECT [System.Id], [System.Title], [System.State], [System.AssignedTo], [System.WorkItemType], [System.TeamProject]
                    FROM workitems 
                    WHERE [System.Title] CONTAINS '{query.strip()}'
                    ORDER BY [System.ChangedDate] DESC
                """
            }
            
            # Use organization-level WIQL query (no project specified)
            wiql_url = f"{AZURE_ORG_URL}/_apis/wit/wiql?api-version=7.1-preview.2"
            wiql_response = requests.post(wiql_url, headers=get_auth_headers(AZURE_PAT), json=wiql_query)
            
            if wiql_response.status_code >= 400:
                return JSONResponse({"error": f"Failed to query work items: {wiql_response.text}"}, status_code=500)
            
            wiql_result = wiql_response.json()
            work_item_ids = [item["id"] for item in wiql_result.get("workItems", [])]
            
            if not work_item_ids:
                return {"work_items": []}
            
            # Limit results
            work_item_ids = work_item_ids[:limit]
            
            # Get detailed work item information
            ids_string = ",".join(map(str, work_item_ids))
            work_items_url = f"{AZURE_ORG_URL}/_apis/wit/workitems?ids={ids_string}&api-version=7.1-preview.3"
            work_items_response = requests.get(work_items_url, headers=get_auth_headers(AZURE_PAT))
            
            if work_items_response.status_code >= 400:
                return JSONResponse({"error": f"Failed to get work items: {work_items_response.text}"}, status_code=500)
            
            work_items_data = work_items_response.json()
            
            # Format work items for frontend
            formatted_work_items = []
            for item in work_items_data.get("value", []):
                fields = item.get("fields", {})
                formatted_work_items.append({
                    "id": item["id"],
                    "title": fields.get("System.Title", ""),
                    "state": fields.get("System.State", ""),
                    "type": fields.get("System.WorkItemType", ""),
                    "project": fields.get("System.TeamProject", ""),
                    "assignedTo": fields.get("System.AssignedTo", {}).get("displayName", "Unassigned") if fields.get("System.AssignedTo") else "Unassigned",
                    "url": item.get("_links", {}).get("html", {}).get("href", "")
                })
            
            return {"work_items": formatted_work_items}
    
    except Exception as e:
        logger.error(f"Error searching work items: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)
