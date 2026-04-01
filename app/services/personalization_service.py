from __future__ import annotations

from typing import Any, Dict, List, Optional


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def normalize_watchlist_items(watchlist: Optional[List[Any]]) -> List[str]:
    normalized: List[str] = []

    for item in watchlist or []:
        symbol = ""

        if isinstance(item, str):
            symbol = item
        elif isinstance(item, dict):
            symbol = (
                item.get("symbol")
                or item.get("ticker")
                or item.get("code")
                or item.get("id")
                or ""
            )

        symbol = _normalize_symbol(symbol)
        if symbol and symbol not in normalized:
            normalized.append(symbol)

    return normalized[:25]


def normalize_portfolio_items(portfolio: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []

    for item in portfolio or []:
        if not isinstance(item, dict):
            continue

        symbol = _normalize_symbol(item.get("symbol"))
        if not symbol:
            continue

        normalized.append(
            {
                "symbol": symbol,
                "weight": item.get("weight"),
                "quantity": item.get("quantity", item.get("shares")),
                "avg_price": item.get("avg_price", item.get("avg_buy_price")),
            }
        )

    return normalized[:50]


def _portfolio_symbols(portfolio: List[Dict[str, Any]]) -> List[str]:
    seen: List[str] = []
    for item in portfolio:
        symbol = _normalize_symbol(item.get("symbol"))
        if symbol and symbol not in seen:
            seen.append(symbol)
    return seen


def _format_watchlist_block(watchlist_symbols: List[str]) -> str:
    if not watchlist_symbols:
        return "No watchlist symbols were provided."

    return ", ".join(watchlist_symbols[:12])


def _format_portfolio_block(portfolio_items: List[Dict[str, Any]]) -> str:
    if not portfolio_items:
        return "No portfolio holdings were provided."

    lines: List[str] = []
    for item in portfolio_items[:12]:
        symbol = item.get("symbol", "UNKNOWN")
        parts: List[str] = []

        if item.get("weight") is not None:
            try:
                parts.append(f"weight={round(float(item['weight']) * 100, 2)}%")
            except Exception:
                pass

        if item.get("quantity") is not None:
            parts.append(f"quantity={item['quantity']}")

        if item.get("avg_price") is not None:
            parts.append(f"avg_price={item['avg_price']}")

        extra = f" ({', '.join(parts)})" if parts else ""
        lines.append(f"- {symbol}{extra}")

    return "\n".join(lines)


def build_personalization_context(
    *,
    question: str,
    symbol: Optional[str] = None,
    compare_symbols: Optional[List[str]] = None,
    portfolio: Optional[List[Dict[str, Any]]] = None,
    watchlist: Optional[List[Any]] = None,
) -> Dict[str, Any]:
    normalized_symbol = _normalize_symbol(symbol)
    normalized_compare_symbols = [
        _normalize_symbol(s) for s in (compare_symbols or []) if _normalize_symbol(s)
    ][:2]

    normalized_watchlist = normalize_watchlist_items(watchlist)
    normalized_portfolio = normalize_portfolio_items(portfolio)
    portfolio_symbols = _portfolio_symbols(normalized_portfolio)

    focus_symbols: List[str] = []
    if normalized_symbol:
        focus_symbols.append(normalized_symbol)
    for sym in normalized_compare_symbols:
        if sym not in focus_symbols:
            focus_symbols.append(sym)

    overlap_watchlist = [sym for sym in focus_symbols if sym in normalized_watchlist]
    overlap_portfolio = [sym for sym in focus_symbols if sym in portfolio_symbols]

    notes: List[str] = []
    if overlap_watchlist:
        notes.append(
            f"Relevant watchlist overlap: {', '.join(overlap_watchlist)}."
        )
    if overlap_portfolio:
        notes.append(
            f"Relevant portfolio overlap: {', '.join(overlap_portfolio)}."
        )
    if normalized_portfolio:
        notes.append(
            f"Portfolio contains {len(normalized_portfolio)} holding(s)."
        )
    if normalized_watchlist:
        notes.append(
            f"Watchlist contains {len(normalized_watchlist)} symbol(s)."
        )

    context_text = f"""
USER PERSONALIZATION CONTEXT

USER QUESTION:
{question}

WATCHLIST SYMBOLS:
{_format_watchlist_block(normalized_watchlist)}

PORTFOLIO HOLDINGS:
{_format_portfolio_block(normalized_portfolio)}

FOCUS SYMBOLS:
{", ".join(focus_symbols) if focus_symbols else "None"}

PERSONALIZATION NOTES:
{chr(10).join(f"- {note}" for note in notes) if notes else "- No strong personalization signals found."}

INSTRUCTIONS:
- Use personalization only as supporting context.
- Mention watchlist or portfolio overlap when directly relevant.
- Do not invent user preferences beyond the provided watchlist and portfolio.
- If a stock is already in the watchlist, mention that as monitoring interest.
- If a stock is already in the portfolio, mention overlap and concentration implications where relevant.
""".strip()

    return {
        "watchlist_symbols": normalized_watchlist,
        "portfolio_items": normalized_portfolio,
        "portfolio_symbols": portfolio_symbols,
        "focus_symbols": focus_symbols,
        "overlap_watchlist": overlap_watchlist,
        "overlap_portfolio": overlap_portfolio,
        "notes": notes,
        "context_text": context_text,
    }