from __future__ import annotations

from typing import Dict, List, Optional

from app.services.yahoo_service import get_quote, get_ratios, get_history
from app.services.recommendation_service import get_stock_recommendation
from app.services.overview_service import get_stock_overview


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _safe_optional(value) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _normalize_symbols(symbols: List[str]) -> List[str]:
    seen = set()
    out = []

    for s in symbols:
        val = (s or "").strip().upper()
        if val and val not in seen:
            out.append(val)
            seen.add(val)

    return out[:4]


# -------------------------
# SNAPSHOT
# -------------------------
def build_compare_snapshot(symbol: str) -> Dict:
    quote = get_quote(symbol) or {}
    ratios = get_ratios(symbol) or {}
    recommendation = get_stock_recommendation(symbol) or {}
    overview = get_stock_overview(symbol) or {}

    return {
        "symbol": symbol,
        "company_name": quote.get("name") or symbol,
        "sector": overview.get("sector"),
        "current_price": _safe_float(quote.get("price")),
        "change": _safe_float(quote.get("change")),
        "change_percent": _safe_float(quote.get("changePercent")),  # ✅ FIXED
        "market_cap": _safe_optional(ratios.get("market_cap")),

        # valuation
        "pe_ratio": _safe_optional(ratios.get("pe_ttm")),  # ✅ FIXED
        "pb_ratio": _safe_optional(ratios.get("pb")),
        "dividend_yield": _safe_optional(ratios.get("dividend_yield")),

        # performance
        "roe": _safe_optional(ratios.get("roe")),
        "debt_to_equity": _safe_optional(ratios.get("debt_to_equity")),
        "profit_margin": _safe_optional(ratios.get("profit_margin")),

        # not available → safe default
        "revenue_growth": None,

        "recommendation": {
            "label": recommendation.get("label", "HOLD"),
            "score": _safe_float(recommendation.get("score")),
        },
        "overview": {
            "summary": overview.get("summary") or "No summary available."
        },
    }


# -------------------------
# HISTORY (CRITICAL FIX)
# -------------------------
def build_compare_history(symbols: List[str], range_: str = "6mo") -> List[Dict]:
    symbol_data = {}

    for symbol in symbols:
        try:
            data = get_history(symbol, range_=range_)
            candles = data.get("candles", [])

            points = {}
            for c in candles:
                date = c.get("t")
                close = _safe_optional(c.get("c"))
                if date and close is not None:
                    points[date] = close

            symbol_data[symbol] = points

        except Exception:
            symbol_data[symbol] = {}

    # merge all dates
    all_dates = sorted(
        {
            d
            for sym_data in symbol_data.values()
            for d in sym_data.keys()
        }
    )

    combined = []

    for date in all_dates:
        row = {}
        for symbol in symbols:
            val = symbol_data[symbol].get(date)
            if val is not None:
                row[symbol] = val

        if row:
            combined.append({
                "date": date,
                "values": row,
            })

    return combined


# -------------------------
# WINNERS
# -------------------------
def _pick_winner(companies: List[Dict], key: str, higher=True) -> Optional[str]:
    valid = [c for c in companies if c.get(key) is not None]
    if not valid:
        return None

    best = max(valid, key=lambda x: x[key]) if higher else min(valid, key=lambda x: x[key])
    return best["symbol"]


def get_metric_winners(companies: List[Dict]) -> Dict[str, Optional[str]]:
    return {
        "pe_ratio": _pick_winner(companies, "pe_ratio", higher=False),
        "pb_ratio": _pick_winner(companies, "pb_ratio", higher=False),
        "dividend_yield": _pick_winner(companies, "dividend_yield", higher=True),
        "roe": _pick_winner(companies, "roe", higher=True),
        "debt_to_equity": _pick_winner(companies, "debt_to_equity", higher=False),
        "profit_margin": _pick_winner(companies, "profit_margin", higher=True),
        "recommendation": _pick_winner(
            [{**c, "score": c["recommendation"]["score"]} for c in companies],
            "score",
            higher=True,
        ),
    }


# -------------------------
# MAIN FUNCTION
# -------------------------
def get_compare_data(symbols: List[str], range_: str = "6mo") -> Dict:
    symbols = _normalize_symbols(symbols)

    companies = [build_compare_snapshot(s) for s in symbols]
    history = build_compare_history(symbols, range_)
    winners = get_metric_winners(companies)

    return {
        "symbols": symbols,
        "companies": companies,
        "history": history,
        "metric_winners": winners,
    }