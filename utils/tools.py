# --- START OF FILE tools.py ---
# my-app/my_agent/utils/tools.py
from utils.singleton import SnowConnect  # Make sure singleton.py is accessible
import logging
import pandas as pd
import os # Added for environment variables
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv # Added

load_dotenv(override=True)

# --- Existing Functions (Keep as is) ---
def get_procedure_ddl(proc_name: str, proc_schema: str) -> str:
    """Gets the procedure DDL using the name and schema."""
    snow_conn = SnowConnect()
    session = snow_conn.getsession()

    query = f"""
        SELECT procedure_definition
        FROM information_schema.procedures
        WHERE procedure_name = '{proc_name.upper()}'
          AND procedure_schema = '{proc_schema.upper()}'
    """
    print(f"Executing: {query}")
    try:
        df = session.sql(query).collect()
        if not df:
            return "Procedure not found in the specified schema."
        return df[0]['PROCEDURE_DEFINITION']
    except Exception as e:
        return f"Error fetching DDL: {e}"

def get_table_ddl(full_table_name: str) -> str:
    """Gets table DDL.  Expects a fully qualified name (schema.table)."""
    snow_conn = SnowConnect()
    session = snow_conn.getsession()
    table_name = full_table_name.replace('"', '')
    query = f"SELECT GET_DDL('TABLE', '{table_name}')"
    print(query)
    try:
        df = session.sql(query).collect()
        return df[0][0]
    except Exception as e:
        return f"Error fetching DDL: {e}"

def execute_snowflake_query(query: str) -> int:
    """Executes a Snowflake query and returns the first integer result."""
    snow_conn = SnowConnect()
    session = snow_conn.getsession()
    try:
        df = session.sql(query).collect()
        # Assuming the query returns a single row with a single integer column
        return int(df[0][0])
    except Exception as e:
        print(f"Error executing query: {e}")
        return -1  # Or some other error indicator

def execute_snowflake_query_to_dataframe(query: str) -> pd.DataFrame | None:
    """Executes a Snowflake query and returns the result as a pandas DataFrame."""
    snow_conn = SnowConnect()
    session = snow_conn.getsession()
    try:
        # Use Snowpark's to_pandas() method
        df = pd.DataFrame(session.sql(query).collect())
        logging.info(f"Successfully executed and fetched DataFrame for query: {query[:100]}...")
        return df
    except Exception as e:
        logging.error(f"Error executing query to DataFrame: {query[:100]}... Error: {e}", exc_info=True)
        return None # Return None to indicate failure

def execute_snowflake_dml(query: str) -> bool:
    """Executes a Snowflake DML statement. Returns True on success, False on failure."""
    snow_conn = SnowConnect()
    session = snow_conn.getsession()
    try:
        session.sql(query).collect()
        return True
    except Exception as e:
        print(f"Error executing DML query: {e}")

def get_pipe_ddl(pipe_name: str, pipe_schema: str) -> str:
    """Gets the Snowpipe DDL using the name and schema."""
    snow_conn = SnowConnect()
    session = snow_conn.getsession()
    full_pipe_name = f"{pipe_schema}.{pipe_name}" # Basic concatenation
    query = f"SELECT GET_DDL('PIPE', '{full_pipe_name}')"
    logging.info(f"Executing: {query}")
    try:
        df = session.sql(query).collect()
        if df and df[0][0]:
            return df[0][0]
        else:
            return f"Error: DDL not found or empty for pipe '{full_pipe_name}'."
    except Exception as e:
        logging.error(f"Error fetching pipe DDL for {full_pipe_name}: {e}", exc_info=True)
        return f"Error fetching pipe DDL: {e}"

def upload_csv_to_azure(csv_content: str, azure_folder_path: str, filename: str) -> bool:
    """Uploads CSV content string to a specific folder in Azure Blob Storage."""
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.getenv("CONTAINER_NAME") # Add this to your .env
    logging.info(f"connection_string{connection_string}")
    logging.info(f"container_name{container_name}")
    if not connection_string or not container_name:
        logging.error("Azure Storage connection string or container name not configured in environment variables.")
        return False

    if not filename.lower().endswith(".csv"):
        logging.warning(f"Filename '{filename}' does not end with .csv. Proceeding anyway.")
        # filename += ".csv" # Optionally enforce

    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        # Ensure container exists (optional, depends on setup)
        # if not container_client.exists():
        #     logging.info(f"Container '{container_name}' does not exist. Creating...")
        #     container_client.create_container()

        # Construct the full blob name including the folder path
        # Ensure folder path doesn't have leading/trailing slashes issues
        folder_path_cleaned = azure_folder_path.strip('/')
        blob_name = f"{folder_path_cleaned}/{filename}" if folder_path_cleaned else filename

        logging.info(f"Attempting to upload to Azure Blob: container='{container_name}', blob='{blob_name}'")

        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(csv_content.encode('utf-8'), overwrite=True)

        logging.info(f"Successfully uploaded '{filename}' to Azure folder '{folder_path_cleaned}'.")
        return True

    except Exception as e:
        logging.error(f"Failed to upload '{filename}' to Azure Blob Storage: {e}", exc_info=True)
        return False
