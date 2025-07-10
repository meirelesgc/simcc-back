from fastapi import APIRouter, Depends
from langchain_core.language_models.chat_models import BaseChatModel

from simcc.core.connection import Connection
from simcc.core.database import get_conn
from simcc.core.model import get_model
from simcc.schemas import DefaultFilters
from simcc.services import ai_service

router = APIRouter()


@router.get('/ai/summary_search/')
async def ai_summary_search(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
    model: BaseChatModel = Depends(get_model),
):
    return await ai_service.ai_summary_search(conn, model, default_filters)
