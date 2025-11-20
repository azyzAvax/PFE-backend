from pydantic import BaseModel
from typing import Dict, Any

class DashboardRequest(BaseModel):
    dashboard_request: str
    business_context: Dict[str, Any]