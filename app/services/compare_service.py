from __future__ import annotations

from typing import Dict, List, Optional

from app.services.yahoo_service import get_history, get_quote, get_ratios
from app.services.overview_service import get_stock_overview
from app.services.recommendation_service import get_stock_recommendation


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _safe_optional(value) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _first_non_null(*values):
    for value in values:
        if value is not None and value != "":
            return value
    return None


def _normalize_symbols(symbols: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []

    for s in symbols:
        val = (s or "").strip().upper()
        if val and val not in seen:
            out.append(val)
            seen.add(val)

    return out[:4]


def build_compare_snapshot(symbol: str) -> Dict:
    symbol = symbol.upper().strip()

    quote = get_quote(symbol) or {}
    ratios = get_ratios(symbol) or {}
    recommendation = get_stock_recommendation(symbol) or {}
    overview = get_stock_overview(symbol) or {}

    quote_block = overview.get("quote") or {}
    ratios_block = overview.get("ratios") or {}
    recommendation_block = overview.get("recommendation") or {}

    market_cap = _first_non_null(
        ratios.get("market_cap"),
        ratios_block.get("market_cap"),
        overview.get("market_cap"),
    )
    pe_ratio = _first_non_null(
        ratios.get("pe_ttm"),
        ratios_block.get("pe_ttm"),
    )
    pb_ratio = _first_non_null(
        ratios.get("pb"),
        ratios_block.get("pb"),
    )
    dividend_yield = _first_non_null(
        ratios.get("dividend_yield"),
        ratios_block.get("dividend_yield"),
    )
    roe = _first_non_null(
        ratios.get("roe"),
        ratios_block.get("roe"),
    )
    debt_to_equity = _first_non_null(
        ratios.get("debt_to_equity"),
        ratios_block.get("debt_to_equity"),
    )
    profit_margin = _first_non_null(
        ratios.get("profit_margin"),
        ratios_block.get("profit_margin"),
    )

    sector = overview.get("sector") or "Unknown"
    company_name = (
        quote.get("name")
        or quote_block.get("name")
        or overview.get("symbol")
        or symbol
    )

    rec_label = (
        recommendation.get("recommendation")
        or recommendation.get("label")
        or recommendation_block.get("recommendation")
        or recommendation_block.get("label")
        or "HOLD"
    )
    rec_score = _first_non_null(
        recommendation.get("score"),
        recommendation_block.get("score"),
        5.0,
    )

    return {
        "symbol": symbol,
        "company_name": company_name,
        "sector": sector,
        "current_price": _safe_float(
            _first_non_null(
                quote.get("price"),
                quote_block.get("price"),
            )
        ),
        "change": _safe_float(
            _first_non_null(
                quote.get("change"),
                quote_block.get("change"),
            )
        ),
        "change_percent": _safe_float(
            _first_non_null(
                quote.get("changePercent"),
                quote_block.get("changePercent"),
            )
        ),
        "market_cap": _safe_optional(market_cap),
        "pe_ratio": _safe_optional(pe_ratio),
        "pb_ratio": _safe_optional(pb_ratio),
        "dividend_yield": _safe_optional(dividend_yield),
        "roe": _safe_optional(roe),
        "debt_to_equity": _safe_optional(debt_to_equity),
        "profit_margin": _safe_optional(profit_margin),
        "revenue_growth": None,
        "recommendation": {
            "label": rec_label,
            "score": _safe_float(rec_score),
        },
        "overview": {
            "summary": overview.get("summary") or "No summary available."
        },
    }


def build_compare_history(symbols: List[str], range_: str = "6mo") -> List[Dict]:
    symbol_data: Dict[str, Dict[str, float]] = {}

    for symbol in symbols:
        try:
            data = get_history(symbol, interval="1d", range_=range_)
            candles = data.get("candles", []) or []

            points: Dict[str, float] = {}
            for c in candles:
                date = c.get("t")
                close = _safe_optional(c.get("c"))
                if date and close is not None:
                    points[date] = close

            symbol_data[symbol] = points
        except Exception:
            symbol_data[symbol] = {}

    all_dates = sorted({d for sym_data in symbol_data.values() for d in sym_data.keys()})

    combined = []
    for date in all_dates:
        row = {}
        for symbol in symbols:
            val = symbol_data.get(symbol, {}).get(date)
            if val is not None:
                row[symbol] = val

        if row:
            combined.append({"date": date, "values": row})

    return combined


def _pick_winner(companies: List[Dict], key: str, higher: bool = True) -> Optional[str]:
    valid = [c for c in companies if c.get(key) is not None]
    if not valid:
        return None

    best = max(valid, key=lambda x: x[key]) if higher else min(valid, key=lambda x: x[key])
    return best["symbol"]


def get_metric_winners(companies: List[Dict]) -> Dict[str, Optional[str]]:
    recommendation_rows = []

    for c in companies:
        rec = c.get("recommendation") or {}
        score = rec.get("score")
        if score is not None:
            recommendation_rows.append({**c, "score": score})

    return {
        "pe_ratio": _pick_winner(companies, "pe_ratio", higher=False),
        "pb_ratio": _pick_winner(companies, "pb_ratio", higher=False),
        "dividend_yield": _pick_winner(companies, "dividend_yield", higher=True),
        "roe": _pick_winner(companies, "roe", higher=True),
        "debt_to_equity": _pick_winner(companies, "debt_to_equity", higher=False),
        "profit_margin": _pick_winner(companies, "profit_margin", higher=True),
        "recommendation": _pick_winner(recommendation_rows, "score", higher=True),
    }


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
