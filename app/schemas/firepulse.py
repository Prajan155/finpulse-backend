from pydantic import BaseModel
from typing import Optional, List, Dict, Any


class FirePulseRequest(BaseModel):
    question: str
    symbol: Optional[str] = None


class FirePulseSource(BaseModel):
    id: str
    label: str
    source: str
    symbol: Optional[str] = None
    topic: Optional[str] = None
    score: Optional[float] = None
    preview: Optional[str] = None


class FirePulseResponse(BaseModel):
    answer: str
    data_used: Dict[str, Any]
    sources: List[FirePulseSource] = []