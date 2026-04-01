# 🔥 UPDATED FIREPULSE CHAT SERVICE (PHASE 3 FINAL)

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from app.services.gemini_service import generate_firepulse_response
from app.services.news_service import fetch_company_news, summarize_news
from app.services.overview_service import get_stock_overview
from app.services.personalization_service import build_personalization_context
from app.services.rag_service import retrieve_context
from app.services.recommendation_service import get_stock_recommendation
from app.services.yahoo_service import get_quote

KNOWLEDGE_DIR = Path("/tmp/knowledge/news")


def load_news_context(symbol: str) -> str:
    try:
        safe = symbol.replace("/", "_").replace("\\", "_").upper()
        path = KNOWLEDGE_DIR / f"{safe}__news.md"

        if not path.exists():
            return ""

        return path.read_text(encoding="utf-8")[:2000]

    except Exception as e:
        print("News context load failed:", e)
        return ""


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


def _safe_get_single_stock_bundle(symbol: str) -> Dict[str, Any]:
    bundle: Dict[str, Any] = {"symbol": symbol}

    try:
        bundle["quote"] = get_quote(symbol)
    except Exception as exc:
        bundle["quote_error"] = str(exc)

    try:
        bundle["overview"] = get_stock_overview(symbol)
    except Exception as exc:
        bundle["overview_error"] = str(exc)

    try:
        bundle["recommendation"] = get_stock_recommendation(symbol)
    except Exception as exc:
        bundle["recommendation_error"] = str(exc)

    return bundle


def answer_firepulse(
    question: str,
    symbol: Optional[str] = None,
    compare_symbols: Optional[List[str]] = None,
    portfolio: Optional[List[Dict[str, Any]]] = None,
    watchlist: Optional[List[Any]] = None,
):
    data: Dict[str, Any] = {}
    mode = "general"

    symbol = symbol.upper().strip() if symbol else None

    if compare_symbols and len(compare_symbols) >= 2:
        mode = "compare"
        compare_symbols = [str(s).upper().strip() for s in compare_symbols[:2] if str(s).strip()]
        data = {
            "compare": [_safe_get_single_stock_bundle(s) for s in compare_symbols]
        }

    elif portfolio:
        mode = "portfolio"
        data = {"portfolio": portfolio, "holding_count": len(portfolio)}

    elif symbol:
        mode = "single"
        data = _safe_get_single_stock_bundle(symbol)

    try:
        rag_items = retrieve_context(question=question, symbol=symbol, top_k=5)
    except Exception as exc:
        print("RAG retrieval failed:", exc)
        rag_items = []

    rag_block = _build_rag_block(rag_items)

    news_summary = ""
    try:
        if symbol:
            news_items = fetch_company_news(symbol)
            news_summary = summarize_news(news_items)
    except Exception as exc:
        print("Live news fetch failed:", exc)
        news_summary = ""

    stored_news = load_news_context(symbol) if symbol else ""

    personalization = build_personalization_context(
        question=question,
        symbol=symbol,
        compare_symbols=compare_symbols,
        portfolio=portfolio,
        watchlist=watchlist,
    )

    prompt = f"""
You are FirePulse AI — a professional stock research analyst.

USER QUESTION:
{question}

MODE:
{mode}

STRUCTURED DATA:
{data}

KNOWLEDGE CONTEXT:
{rag_block}

LATEST NEWS:
{news_summary}

PERSISTENT NEWS INTELLIGENCE:
{stored_news}

PERSONALIZATION:
{personalization.get("context_text", "")}

INSTRUCTIONS:
- Use real data only
- Combine structured data, knowledge context, latest news, and persistent news when relevant
- Give clear verdicts
- Be concise and insightful
- Do not invent numbers
- End with "Sources used"

FORMAT:
1. Business Overview
2. Investment Verdict
3. Key Strengths
4. Key Risks
5. Technical + Fundamental Insight
6. Confidence Level
7. Sources used
""".strip()

    # ================= 🔥 GEMINI =================

    if len(question.strip()) < 5:
        answer = "Please provide a more detailed question."
    else:
        try:
            answer = generate_firepulse_response(prompt)
        except Exception:
            print("⚠️ Using fallback instead of Gemini")

            if mode == "single" and isinstance(data.get("quote"), dict):
                price = data["quote"].get("price")
                change_percent = data["quote"].get("changePercent")
                recommendation = (data.get("recommendation") or {}).get("recommendation", "N/A")

                answer = f"""
                🔥 FirePulse Insight (Lite Mode)

                • Current Price: {round(price, 2) if price else "N/A"}
                • Change: {round(change_percent, 2) if change_percent else "N/A"}%
                • Signal: {recommendation}

                ⚡ AI analysis is temporarily limited due to high demand. Core insights will resume shortly.
                """
            elif mode == "compare":
                answer = "FirePulse AI is temporarily in low-power mode. Compare data is available, but detailed AI reasoning is unavailable right now."
            elif mode == "portfolio":
                holding_count = data.get("holding_count", 0)
                answer = f"FirePulse AI is temporarily in low-power mode. Portfolio data is available for {holding_count} holding(s), but detailed AI reasoning is unavailable right now."
            else:
                answer = "FirePulse AI is temporarily in low-power mode. Try again shortly."

    sources = [
        {
            "label": item.get("label"),
            "symbol": item.get("symbol"),
            "score": item.get("score"),
        }
        for item in rag_items
    ]

    return {
        "answer": answer,
        "mode": mode,
        "data_used": data,
        "sources": sources,
    }