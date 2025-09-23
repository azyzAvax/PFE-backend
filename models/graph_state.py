from dataclasses import dataclass
from typing import Optional, Dict

@dataclass
class GraphState:
    metadata: Optional[Dict] = None
    extracted_data: Optional[str] = None
    usp_template: Optional[str] = None
@dataclass
class UDFGraphState:
    metadata: Optional[Dict] = None
    udf_template: Optional[str] = None