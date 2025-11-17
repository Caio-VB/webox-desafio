from typing import Dict, Any
from pydantic import BaseModel


Row = Dict[str, Any]


class AskRequest(BaseModel):
    question: str


class AgentResponse(BaseModel):
    answer: str
    debug_sql: str | None = None
