from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class FirePulseRequest(BaseModel):
    question: str
    symbol: Optional[str] = None
    compare_symbols: Optional[List[str]] = None
    portfolio: Optional[List[Dict[str, Any]]] = None
    watchlist: Optional[List[Any]] = None


class FirePulseSource(BaseModel):
    id: Optional[str] = None
    label: Optional[str] = None
    source: Optional[str] = None
    symbol: Optional[str] = None
    topic: Optional[str] = None
    score: Optional[float] = None
    preview: Optional[str] = None


class FirePulseResponse(BaseModel):
    answer: str
    mode: Optional[str] = None
    data_used: Dict[str, Any] = Field(default_factory=dict)
    sources: List[FirePulseSource] = Field(default_factory=list)