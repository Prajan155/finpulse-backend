from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.services.gemini_service import generate_firepulse_response
from app.services.overview_service import get_stock_overview
from app.services.rag_service import retrieve_context
from app.services.recommendation_service import get_stock_recommendation
from app.services.yahoo_service import get_quote


def _build_rag_block(items: List[Dict[str, Any]]) -> str:
    if not items:
        return "No retrieved knowledge base context was found."

    parts = []
    for idx, item in enumerate(items, start=1):
        content = (item.get("text") or "")[:400].strip()

        parts.append(
            f"""[Context {idx}]
Source Label: {item.get("label")}
Symbol: {item.get("symbol")}
Topic: {item.get("topic")}
Similarity distance: {item.get("score")}

Content:
{content}
"""
        )
    return "\n\n".join(parts)


def build_prompt(question: str, data: Dict[str, Any], rag_items: List[Dict[str, Any]]) -> str:
    rag_block = _build_rag_block(rag_items)

    return f"""
You are FirePulse AI — a professional stock research analyst inside FinPulse Nexus.

STRICT RULES:
- Use STRUCTURED DATA for all numbers, metrics, prices, ratios, and recommendation signals.
- Use KNOWLEDGE CONTEXT for company/business explanations, qualitative context, and risks.
- If structured data and knowledge context disagree, trust the structured data for current numbers.
- NEVER invent financial values.
- Be concise, insightful, and professional.
- Speak like an equity research analyst.
- If evidence is limited, clearly say so.
- End with a brief "Sources used" section.

USER QUESTION:
{question}

STRUCTURED DATA:
{data}

KNOWLEDGE CONTEXT:
{rag_block}

OUTPUT FORMAT:

1. Business Overview
2. Investment Verdict (Buy / Hold / Sell with reasoning)
3. Key Strengths
4. Key Risks
5. Technical + Fundamental Insight
6. Confidence Level (Low / Medium / High)
7. Sources used
""".strip()


def _format_source(item: Dict[str, Any]) -> Dict[str, Any]:
    path = item.get("source", "") or ""
    name = path.split("\\")[-1].split("/")[-1]
    name = name.replace(".txt", "").replace(".md", "").replace(".json", "")
    label = name.replace("__", " - ")

    preview_text = (item.get("text") or "").strip()
    preview = (preview_text[:160] + "...") if len(preview_text) > 160 else preview_text

    return {
        "id": item["id"],
        "label": label,
        "source": path,
        "symbol": item.get("symbol"),
        "topic": item.get("topic"),
        "score": item.get("score"),
        "preview": preview or None,
    }


def answer_firepulse(question: str, symbol: Optional[str] = None):
    data: Dict[str, Any] = {}
    normalized_symbol = symbol.upper().strip() if symbol else None

    if normalized_symbol:
        quote = get_quote(normalized_symbol)
        overview = get_stock_overview(normalized_symbol)
        recommendation = get_stock_recommendation(normalized_symbol)

        data = {
            "quote": quote,
            "overview": overview,
            "recommendation": recommendation,
        }

    rag_items = retrieve_context(question=question, symbol=normalized_symbol, top_k=5)
    prompt = build_prompt(question=question, data=data, rag_items=rag_items)
    answer = generate_firepulse_response(prompt)

    sources = [_format_source(item) for item in rag_items]

    return {
        "answer": answer,
        "data_used": data,
        "sources": sources,
    }