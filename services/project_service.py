from fastapi.responses import JSONResponse
from pydantic import BaseModel
from datetime import datetime
import uuid
import requests
from utils.azure_utils import get_auth_headers
import logging

logger = logging.getLogger("uvicorn.error")

class ProjectInit(BaseModel):
    project_name: str
    description: str = ""
    project_type: str = "snowflake"

async def initialize_project(project: ProjectInit, env_vars: dict):
    """
    Initialize a new project with a name, description, and type.
    Creates a branch in the repository for this project.
    """
    try:
        # Generate a unique ID for the project
        project_id = str(uuid.uuid4())
        
        # Create a branch name based on the project name
        branch_name = f"project-{project.project_name.lower().replace(' ', '-')}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Get latest commit from main
        refs_url = f"{env_vars['AZURE_ORG_URL']}/{env_vars['AZURE_PROJECT']}/_apis/git/repositories/{env_vars['AZURE_REPO_ID']}/refs?filter=heads/main&api-version=7.1-preview.1"
        refs_res = requests.get(refs_url, headers=get_auth_headers(env_vars['AZURE_PAT'])).json()
        main_commit = refs_res["value"][0]["objectId"]
        
        # Create new branch
        create_branch_url = f"{env_vars['AZURE_ORG_URL']}/{env_vars['AZURE_PROJECT']}/_apis/git/repositories/{env_vars['AZURE_REPO_ID']}/refs?api-version=7.1-preview.1"
        branch_data = [{
            "name": f"refs/heads/{branch_name}",
            "oldObjectId": "0000000000000000000000000000000000000000",
            "newObjectId": main_commit
        }]
        branch_res = requests.post(create_branch_url, headers=get_auth_headers(env_vars['AZURE_PAT']), json=branch_data)
        
        if branch_res.status_code >= 400:
            return JSONResponse({"detail": f"Failed to create branch: {branch_res.text}"}, status_code=500)
        
        # Create a README file in the branch
        readme_content = f"# {project.project_name}\n\n{project.description}\n\nProject Type: {project.project_type}\nCreated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        commit_url = f"{env_vars['AZURE_ORG_URL']}/{env_vars['AZURE_PROJECT']}/_apis/git/repositories/{env_vars['AZURE_REPO_ID']}/pushes?api-version=7.1-preview.2"
        push_data = {
            "refUpdates": [{
                "name": f"refs/heads/{branch_name}",
                "oldObjectId": main_commit
            }],
            "commits": [{
                "comment": f"Initialize project: {project.project_name}",
                "changes": [{
                    "changeType": "add",
                    "item": {"path": "/README.md"},
                    "newContent": {
                        "content": readme_content,
                        "contentType": "rawtext"
                    }
                }]
            }]
        }
        commit_res = requests.post(commit_url, headers=get_auth_headers(env_vars['AZURE_PAT']), json=push_data)
        
        if commit_res.status_code >= 400:
            return JSONResponse({"detail": f"Failed to create README: {commit_res.text}"}, status_code=500)
        
        # Save project info
        project_info = {
            "id": project_id,
            "name": project.project_name,
            "description": project.description,
            "type": project.project_type,
            "branch": branch_name,
            "created_at": datetime.now().isoformat(),
            "files": []
        }
        
        return project_info
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=500)

async def list_active_projects(env_vars: dict):
    """List active projects in Azure DevOps."""
    repos_url = f"{env_vars['AZURE_ORG_URL']}/{env_vars['AZURE_PROJECT']}/_apis/git/repositories?api-version=7.1-preview.1"
    try:
        logger.info(f"Azure DevOps response pat: {env_vars['AZURE_PAT']}")
        response = requests.get(repos_url, headers=get_auth_headers(env_vars['AZURE_PAT']))
        logger.info(f"Azure DevOps response code: {response.status_code}")
     
        if response.status_code >= 400:
            return JSONResponse({"detail": f"Failed to fetch repositories: {response.text}"}, status_code=502)

        repos_data = response.json()
        projects_list = []
        for repo in repos_data.get("value", []):
            projects_list.append({
                "id": repo.get("id"),
                "name": repo.get("name"),
                "url": repo.get("webUrl"),
                "defaultBranch": repo.get("defaultBranch"),
                "isDisabled": repo.get("isDisabled"),
                "size": repo.get("size"),
            })

        return {"projects": projects_list}
    except requests.RequestException as e:
        logger.error(f"Request to Azure DevOps failed: {e}")
        return JSONResponse({"detail": f"Request failed: {e}"}, status_code=501)
    except Exception as e:
        logger.error(f"Failed to parse JSON response: {e}")
        return JSONResponse({"detail": f"Failed to parse JSON response: {e}"}, status_code=503)