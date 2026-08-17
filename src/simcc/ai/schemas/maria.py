from typing import List, Optional, Any, Dict

from pydantic import BaseModel

from simcc.schemas.researcher import Researcher


class MariaResponse(BaseModel):
    query: str
    researchers: List[Researcher]


class ChatRequest(BaseModel):
    query: str
    session_id: Optional[str] = None


class ChatResponse(BaseModel):
    answer: str
    intent: str
    filters_extracted: Dict[str, Any]
    researchers: List[Dict[str, Any]]
    productions: List[Dict[str, Any]]
    sources: List[str]
