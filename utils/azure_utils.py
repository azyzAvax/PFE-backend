import hashlib
import base64
import yaml
import os
from typing import List
import logging

logger = logging.getLogger("uvicorn.error")

def get_auth_headers(pat: str) -> dict:
    """Generate authentication headers for Azure DevOps."""
    pat_bytes = f":{pat}".encode()
    pat_encoded = base64.b64encode(pat_bytes).decode()
    return {
        "Authorization": f"Basic {pat_encoded}",
        "Content-Type": "application/json"
    }

def generate_changelog_sql(sql_files: List[str], changelog_path: str, object_type: str, author: str, context_filter: str, project_env: str, project_name: str, context_dir: str) -> str:
    """
    Generates or updates a Liquibase changelog YAML by appending new changesets for new files.
    Preserves existing changesets and adds new ones for unique filenames.
    All parameters must be provided by the caller.
    """
    if not all([project_env, project_name, context_dir, author, context_filter]):
        raise ValueError("project_env, project_name, context_dir, author, and context_filter must be provided")

    # Mapping of object types to Liquibase ID prefixes based on attachment
    object_type_prefixes = {
        "fileformat": "10", "table": "20", "stream": "30", "udf": "40", "view": "50",
        "snowpipe": "60", "procedure": "70", "task": "80", "dcl": "91"
    }
    
    base_id_prefix = object_type_prefixes.get(object_type.lower(), "70")  # Default to procedure
    
    # Check if changelog exists and load existing changes
    existing_changes = []
    if os.path.exists(changelog_path):
        with open(changelog_path, "r") as f:
            existing_data = yaml.safe_load(f) or {"databaseChangeLog": []}
            existing_changes = existing_data.get("databaseChangeLog", [])
        logger.info(f"Loaded existing changelog from {changelog_path} with {len(existing_changes)} changesets")

    changes = existing_changes.copy()
    for sql_file in sql_files:
        filename = os.path.basename(sql_file)
        name_without_ext = filename.replace(".sql", "").replace("_", "-")
        
        # Generate unique change ID based on filename
        change_id = f"{base_id_prefix}_{name_without_ext}" if object_type.lower() == "udf" else f"{base_id_prefix}_{object_type.lower()}-{name_without_ext}"
        
        # Add changeSet if not already presentnpm start
        if not any(change["changeSet"]["id"] == change_id for change in changes):
            changes.append({
                "changeSet": {
                    "id": change_id,
                    "author": author,
                    "runOnChange": True,
                    "contextFilter": context_filter,
                    "changes": [
                        {
                            "sqlFile": {
                                "path": f"../snowflake/dataplatform/ddl/{context_dir}/{base_id_prefix}_{object_type.lower()}/{filename}"
                            }
                        }
                    ]
                }
            })
            # Add endDelimiter only for procedure object type
            if object_type.lower() == "procedure":
                changes[-1]["changeSet"]["changes"][0]["sqlFile"]["endDelimiter"] = ""
            logger.info(f"Added new changeSet for {filename} with id {change_id}")
        else:
            logger.info(f"Existing changeSet found for {filename} with id {change_id}, will be reapplied with runOnChange")

    changelog_content = {"databaseChangeLog": changes}
    os.makedirs(os.path.dirname(changelog_path), exist_ok=True)
    with open(changelog_path, "w") as f:
        yaml.dump(changelog_content, f, sort_keys=False)
    logger.info(f"Changelog generated/updated at {changelog_path}")
    return changelog_path

def validate_env_vars(env_vars: dict, debug_mode: bool = False) -> bool:
    """Validate required environment variables for Azure deployment, optional in debug mode."""
    if debug_mode:
        logger.warning("Debug mode enabled: Skipping environment variable validation.")
        return True
    required = ['AZURE_ORG_URL', 'AZURE_PROJECT', 'AZURE_REPO_ID', 'AZURE_PAT', 'PROJECT_ENV', 'PROJECT_NAME']
    missing = [var for var in required if not env_vars.get(var)]
    if missing:
        logger.error(f"Missing environment variables: {missing}")
        return False
    return True

 