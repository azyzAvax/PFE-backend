# langgraph_agent/utils/state.py
from typing import TypedDict, List, Dict
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

class GraphState(TypedDict):
    procedure_name: str
    procedure_schema: str
    procedure_ddl: str
    ddl_data: List[Dict[str, str]]
    messages: List[AIMessage | HumanMessage | SystemMessage]
    unit_tests: List[Dict[str, str]]  # Store unit tests as a list of dictionaries
    test_results: List[Dict[str, str]]  # Store test results
    truncated: bool #To know if the table has been truncated or not