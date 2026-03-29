from fastapi import APIRouter

from app.schemas.firepulse import FirePulseRequest, FirePulseResponse
from app.services.firepulse_chat_service import answer_firepulse
from app.services.rag_ingest_service import ingest_knowledge_base

router = APIRouter()


@router.post("/chat", response_model=FirePulseResponse)
def firepulse_chat(payload: FirePulseRequest):
    return answer_firepulse(payload.question, payload.symbol)


@router.post("/reindex")
def firepulse_reindex():
    return ingest_knowledge_base()