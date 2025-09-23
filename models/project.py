from pydantic import BaseModel,Field

class ProjectInit(BaseModel):
    project_name: str
    description: str = ""
    project_type: str = "snowflake"
    

class ProcedureInput(BaseModel):
    procedure_name: str = Field(..., description="The name of the Snowflake stored procedure.")
    procedure_schema: str = Field(..., description="The schema of the Snowflake stored procedure.")
class PipeInput(BaseModel): # New model for Pipe testing
    pipe_name: str = Field(..., description="The name of the Snowflake Snowpipe.")
    pipe_schema: str = Field(..., description="The schema of the Snowflake Snowpipe.")