# langgraph_agent/utils/pipe_state.py
from typing import TypedDict, List, Dict, Optional
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
import pandas as pd

class PipeGraphState(TypedDict):
    pipe_name: str
    pipe_schema: str
    pipe_ddl: Optional[str]
    target_table_name: Optional[str]
    target_table_ddl: Optional[str]
    azure_folder_path: Optional[str]
    generated_csv_content: Optional[str]
    generated_csv_filename: Optional[str]
    upload_status: Optional[bool]
    verification_query: Optional[str]
    verification_result: Optional[int]
    target_table_data_after_test: Optional[pd.DataFrame]
    final_message: Optional[str]
    error_message: Optional[str]
    messages: List[AIMessage | HumanMessage | SystemMessage]