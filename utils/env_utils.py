from dotenv import load_dotenv
import os

def get_env_vars():
    """Load environment variables from .env file."""
    load_dotenv(override=True)
    return {
        "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
        "AZURE_PAT": os.getenv("AZURE_PAT"),
        "AZURE_ORG_URL": os.getenv("AZURE_ORG_URL"),
        "AZURE_PROJECT": os.getenv("AZURE_PROJECT"),
        "AZURE_REPO_ID": os.getenv("AZURE_REPO_ID")
    }