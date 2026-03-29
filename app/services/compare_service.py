from __future__ import annotations

from typing import Dict, List, Optional
import yfinance as yf

from app.services.yahoo_service import get_quote, get_ratios, get_history
from app.services.recommendation_service import get_stock_recommendation
from app.services.overview_service import get_stock_overview


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
    out = []

    for s in symbols:
        val = (s or "").strip().upper()
        if val and val not in seen:
            out.append(val)
            seen.add(val)

    return out[:4]


# 🔥 NEW: DIRECT FALLBACK
def _fetch_direct_info(symbol: str):
    try:
        t = yf.Ticker(symbol)
        return t.info or {}
    except Exception:
        return {}


# -------------------------
# SNAPSHOT
# -------------------------
def build_compare_snapshot(symbol: str) -> Dict:
    quote = get_quote(symbol) or {}
    ratios = get_ratios(symbol) or {}
    recommendation = get_stock_recommendation(symbol) or {}
    overview = get_stock_overview(symbol) or {}

    # 🔥 FINAL FALLBACK SOURCE
    direct_info = _fetch_direct_info(symbol)

    # -------------------------
    # FIXED FALLBACKS
    # -------------------------
    market_cap = _first_non_null(
        ratios.get("market_cap"),
        quote.get("marketCap"),
        quote.get("market_cap"),
        direct_info.get("marketCap"),
    )

    pe_ratio = _first_non_null(
        ratios.get("pe_ttm"),
        ratios.get("pe"),
        quote.get("trailingPE"),
        quote.get("forwardPE"),
        direct_info.get("trailingPE"),
    )

    pb_ratio = _first_non_null(
        ratios.get("pb"),
        quote.get("priceToBook"),
        direct_info.get("priceToBook"),
    )

    dividend_yield = _first_non_null(
        ratios.get("dividend_yield"),
        quote.get("dividendYield"),
        direct_info.get("dividendYield"),
    )

    roe = _first_non_null(
        ratios.get("roe"),
        quote.get("returnOnEquity"),
        direct_info.get("returnOnEquity"),
    )

    debt_to_equity = _first_non_null(
        ratios.get("debt_to_equity"),
        quote.get("debtToEquity"),
        direct_info.get("debtToEquity"),
    )

    profit_margin = _first_non_null(
        ratios.get("profit_margin"),
        quote.get("profitMargins"),
        direct_info.get("profitMargins"),
    )

    sector = (
        overview.get("sector")
        or quote.get("sector")
        or direct_info.get("sector")
        or "Unknown"
    )

    company_name = (
        quote.get("name")
        or overview.get("name")
        or direct_info.get("longName")
        or symbol
    )

    return {
        "symbol": symbol,
        "company_name": company_name,
        "sector": sector,
        "current_price": _safe_float(
            _first_non_null(
                quote.get("price"),
                quote.get("currentPrice"),
                quote.get("regularMarketPrice"),
                direct_info.get("regularMarketPrice"),
            )
        ),
        "change": _safe_float(
            _first_non_null(
                quote.get("change"),
                quote.get("regularMarketChange"),
                direct_info.get("regularMarketChange"),
            )
        ),
        "change_percent": _safe_float(
            _first_non_null(
                quote.get("changePercent"),
                quote.get("regularMarketChangePercent"),
                direct_info.get("regularMarketChangePercent"),
            )
        ),
        "market_cap": _safe_optional(market_cap),

        "pe_ratio": _safe_optional(pe_ratio),
        "pb_ratio": _safe_optional(pb_ratio),
        "dividend_yield": _safe_optional(dividend_yield),

        "roe": _safe_optional(roe),
        "debt_to_equity": _safe_optional(debt_to_equity),
        "profit_margin": _safe_optional(profit_margin),

        "revenue_growth": _safe_optional(
            _first_non_null(
                ratios.get("revenue_growth"),
                quote.get("revenueGrowth"),
                direct_info.get("revenueGrowth"),
            )
        ),

        "recommendation": {
            "label": recommendation.get("label", "HOLD"),
            "score": _safe_float(recommendation.get("score")),
        },
        "overview": {
            "summary": overview.get("summary") or "No summary available."
        },
    }


# -------------------------
# HISTORY
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