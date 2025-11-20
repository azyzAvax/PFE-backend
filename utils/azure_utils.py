import base64
import yaml
import os
from typing import List, Optional
import logging

logger = logging.getLogger("uvicorn.error")

def get_auth_headers(pat: str) -> dict:
    """Generate authentication headers for Azure DevOps."""
    pat_bytes = f":{pat}".encode()
    pat_encoded = base64.b64encode(pat_bytes).decode()
    return {"Authorization": f"Basic {pat_encoded}", "Content-Type": "application/json"}

def generate_changelog_sql(
    sql_files: List[str],
    changelog_path: str,
    obj_type: Optional[str] = None,
    author: Optional[str] = None,
    context_filter: Optional[str] = None,
    project_env: Optional[str] = None,
    project_name: Optional[str] = None,
) -> str:
    """
    Generates or updates a Liquibase changelog YAML based on a list of SQL file paths.
    For task and view: One changeSet per file with runAlways.
    For udf and procedure: One changeSet per file with endDelimiter.
    Optional metadata parameters can be inferred when not explicitly provided.
    """
    valid_sql_files = [path for path in sql_files if path]
    if not valid_sql_files:
        raise ValueError("At least one SQL file path must be provided")

    first_sql_file = valid_sql_files[0]
    normalized_path = first_sql_file.replace("\\", "/")

    path_parts = normalized_path.split("/")
    env_from_path: Optional[str] = None
    context_from_path: Optional[str] = None
    inferred_obj_type: Optional[str] = None

    if "temp" in path_parts:
        try:
            env_index = path_parts.index("temp") + 1
            candidate_env = path_parts[env_index]
            env_from_path = candidate_env if "." not in candidate_env else None
        except (ValueError, IndexError):
            env_from_path = None

    if "ddl" in path_parts:
        ddl_index = path_parts.index("ddl")
        if len(path_parts) > ddl_index + 1:
            context_from_path = path_parts[ddl_index + 1]
        if len(path_parts) > ddl_index + 2:
            type_folder = path_parts[ddl_index + 2]
            if "_" in type_folder:
                inferred_obj_type = type_folder.split("_", 1)[-1]
            else:
                inferred_obj_type = type_folder

    obj_type = (obj_type or inferred_obj_type or "procedure").lower()
    author = author or os.getenv("LIQUIBASE_AUTHOR") or "system"
    project_env = project_env or env_from_path or os.getenv("PROJECT_ENV")
    project_name = project_name or os.getenv("PROJECT_NAME")
    # Use provided context filter, fall back to context derived from path, project env, then default.
    context_filter = context_filter or context_from_path or project_env or "default"

    object_type_prefixes = {
        "fileformat": "10", "table": "20", "stream": "30", "udf": "40", "view": "50",
        "snowflake": "60", "procedure": "70", "task": "80", "stage": "00"
    }
    
    base_id_prefix = object_type_prefixes.get(obj_type.lower(), "70")
    
    # Load existing changelog to track existing IDs
    existing_changes = {}
    if os.path.exists(changelog_path):
        try:
            with open(changelog_path, "r", encoding="utf-8") as f:
                existing_data = yaml.safe_load(f)
                if existing_data and isinstance(existing_data, dict) and "databaseChangeLog" in existing_data:
                    for change in existing_data["databaseChangeLog"]:
                        if isinstance(change, dict) and "changeSet" in change and "id" in change["changeSet"]:
                            existing_changes[change["changeSet"]["id"].strip('"')] = change
                else:
                    logger.warning(f"Invalid YAML structure in {changelog_path}, initializing with empty changelog")
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse {changelog_path}: {str(e)}, initializing with empty changelog")
    logger.info(f"Loaded existing changelog from {changelog_path} with {len(existing_changes)} valid changesets")

    # Build new changelog content
    changelog_lines = ["databaseChangeLog:"]
    processed_ids = set()

    # Process new SQL files
    for sql_file in sql_files:
        if sql_file:  # Skip empty paths
            filename = os.path.basename(sql_file)
            name_without_ext = filename.replace(".sql", "").replace("_", "-").replace(".", "-")
            change_id = f'"{base_id_prefix}_{obj_type.lower()}-{name_without_ext}"' if obj_type.lower() != "udf" else f'"{base_id_prefix}_{name_without_ext}"'
            relative_path = os.path.relpath(sql_file, os.path.dirname(changelog_path)).replace("\\", "/")
            
            change_set_lines = [
                "  - changeSet:",
                f"      id: {change_id}",
                f"      author: {author}",
                "      runOnChange: true",
                f"      contextFilter: {context_filter}"
            ]
            # Add runAlways for task and view
            if obj_type.lower() in ["task", "view"]:
                change_set_lines.append("      runAlways: true")
            
            change_set_lines.extend([
                "      changes:",
                "        - sqlFile:",
                f"            path: \"../snowflake/dataplatform/ddl/{relative_path.split('ddl/')[-1] if 'ddl/' in relative_path else relative_path}\""
            ])
            if obj_type.lower() in ["udf", "procedure"]:
                change_set_lines.append("            endDelimiter: \"\"")
            
            changelog_lines.extend(change_set_lines)
            processed_ids.add(change_id.strip('"'))
            logger.info(f"Processed new changeSet for {filename} with id {change_id}")

    # Process existing changes, reformatting with quoted paths
    for change_id, change in existing_changes.items():
        if change_id not in processed_ids:
            change_set_lines = ["  - changeSet:"]
            for key, value in change["changeSet"].items():
                if key != "changes":  # Skip existing changes block to reformat
                    change_set_lines.append(f"      {key}: {value}")
            change_set_lines.append("      changes:")
            if "changes" in change["changeSet"] and isinstance(change["changeSet"]["changes"], list):
                for change_item in change["changeSet"]["changes"]:
                    if isinstance(change_item, dict) and "sqlFile" in change_item and isinstance(change_item["sqlFile"], dict) and "path" in change_item["sqlFile"]:
                        sql_file_lines = ["        - sqlFile:"]
                        quoted_path = f"\"{change_item['sqlFile']['path']}\"" if not change_item['sqlFile']['path'].startswith('"') else change_item['sqlFile']['path']
                        sql_file_lines.append(f"            path: {quoted_path}")
                        change_set_lines.extend(sql_file_lines)
            changelog_lines.extend(change_set_lines)
            logger.info(f"Reformatted existing changeSet with id {change_id}")

    # Write the changelog
    changelog_content = "\n".join(changelog_lines) if changelog_lines else "databaseChangeLog:"
    os.makedirs(os.path.dirname(changelog_path), exist_ok=True)
    with open(changelog_path, "w", encoding="utf-8") as f:
        f.write(changelog_content)
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
