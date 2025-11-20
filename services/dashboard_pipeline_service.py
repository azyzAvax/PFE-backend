# services/dashboard_pipeline_service.py
from utils.llm_manager import  LLMSingleton
from langchain.prompts import PromptTemplate
import json, logging
from typing import Dict, Any
logger = logging.getLogger("uvicorn.error")
import traceback
from utils.llm_manager import  LLMSingleton
llm = LLMSingleton().get_llm()
class DashboardPipelineService:

    def generate_pipeline(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
            try:
                user_prompt = request_data.get("dashboard_prompt", "")

                # --------- Agent 1: Extract pipeline logic
                logic_prompt = PromptTemplate.from_template(self._logic_prompt_template())
                logic_response = llm.invoke(logic_prompt.format(dashboard_request=user_prompt))

                try:
                    parsed_pipeline = json.loads(logic_response.content.strip())
                except Exception as e:
                    logger.error(f"[Agent1] Failed to parse JSON: {str(e)}\nRaw:\n{logic_response.content}")
                    raise ValueError("Invalid pipeline JSON format from LLM")

                # --------- Agent 2: Generate diagram and explanations
                explain_prompt = PromptTemplate.from_template(self._explain_prompt_template())
                explain_response = llm.invoke(explain_prompt.format(pipeline_json=json.dumps(parsed_pipeline)))

                try:
                    parsed_explain = json.loads(explain_response.content.strip())
                except Exception as e:
                    logger.error(f"[Agent2] Failed to parse explanation JSON: {str(e)}\nRaw:\n{explain_response.content}")
                    raise ValueError("Invalid diagram/explanation format from LLM")

                return {
                    "success": True,
                    "flow": parsed_pipeline,
                    "diagram": parsed_explain.get("mermaid", ""),
                    "explanations": parsed_explain.get("explanations", [])
                }

            except Exception as e:
                logger.error(f"Pipeline generation failed: {str(e)}\n{traceback.format_exc()}")
                return {"success": False, "error": str(e)}

    def _logic_prompt_template(self) -> str:
        return """
    You are a data pipeline architect AI.

    Based on the user's dashboard request, design a pipeline to generate the necessary datamart.

    Extract:
    - The **KPIs** and **dimensions**
    - The required **source tables**
    - All intermediate steps required to prepare the data (e.g., joins, filters, aggregations)
    - A **final view** or table ready for dashboarding

    Return ONLY JSON like:
    {{
    "kpis": [...],
    "dimensions": [...],
    "steps": [
        {{
        "step_name": "Extract Orders",
        "source_tables": ["orders"],
        "logic": "SELECT * FROM orders WHERE order_date > CURRENT_DATE - INTERVAL '30 days'"
        }},
        ...
    ],
    "final_view": {{
        "name": "dm_sales_dashboard",
        "sql": "CREATE OR REPLACE VIEW dm_sales_dashboard AS SELECT ...;"
    }}
    }}

    User dashboard request:
    {dashboard_request}
    """

    def _explain_prompt_template(self) -> str:
        return """
    You are a data pipeline explainer AI.

    Given the following pipeline JSON, do the following:

    1. Convert the `steps` list into a Mermaid `flowchart LR` diagram (horizontal flow).
    2. For each step, assign a unique node ID (A, B, C...) and define the node like: A[Extract Orders].
    3. Connect the steps in order using arrows (-->).
    4. For each step, provide a plain-language explanation of what the logic does.

    Return pure JSON in the following format:
    {{
    "mermaid": "flowchart LR\\nA[Extract Orders] --> B[Extract Customers] --> C[Calculate Revenue]",
    "explanations": [
        {{"step": "Extract Orders", "description": "Fetches recent orders for the last 30 days."}},
       {{"step": "Extract Customers", "description": "Retrieves all customers from the database."}}
    ]
    }}

    Pipeline JSON:
    {pipeline_json}

    ###Return ONLY JSON. Do NOT include triple backticks or markdown code fences.Return pure JSON, without markdown formatting.###
    """