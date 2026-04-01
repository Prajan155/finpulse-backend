from __future__ import annotations

from app.services.recommendation_service import get_stock_recommendation
from app.services.yahoo_service import (
    get_company_profile,
    get_long_range_forecast_arima,
    get_quote,
    get_ratios,
    get_revenue_expenses,
)


def _fallback_recommendation(symbol: str, price: float | None) -> dict:
    current_price = float(price) if price is not None else 0.0
    return {
        "symbol": symbol,
        "regime": "balanced",
        "sector": "Unknown",
        "recommendation": "HOLD",
        "score": 5.0,
        "confidence": 0.35,
        "current_price": current_price,
        "target_price": current_price,
        "upside_percent": 0.0,
        "signals": {
            "technical": "neutral",
            "fundamental": "neutral",
            "sector": "neutral",
            "growth": "neutral",
            "forecast": "neutral",
        },
        "reasons": [
            "Live market data is temporarily limited",
            "Using fallback recommendation until richer data is available",
        ],
    }


def get_stock_overview(symbol: str) -> dict:
    symbol = symbol.upper().strip()

    quote = get_quote(symbol) or {}
    profile = get_company_profile(symbol) or {}

    price = quote.get("price")
    has_live_price = price not in (None, 0, 0.0)

    ratios = get_ratios(symbol) if has_live_price else {
        "symbol": symbol,
        "market_cap": profile.get("market_cap"),
        "pe_ttm": None,
        "pb": None,
        "ps_ttm": None,
        "dividend_yield": None,
        "beta": None,
        "profit_margin": None,
        "operating_margin": None,
        "roe": None,
        "roa": None,
        "debt_to_equity": None,
        "current_ratio": None,
        "quick_ratio": None,
    }

    recommendation = (
        get_stock_recommendation(symbol) if has_live_price else _fallback_recommendation(symbol, price)
    )

    forecast = (
        get_long_range_forecast_arima(symbol, horizon_days=30, interval="1d")
        if has_live_price
        else {
            "symbol": symbol,
            "model": "ARIMA",
            "interval": "1d",
            "horizon_days": 30,
            "last_close": None,
            "forecast_close": None,
            "error": "Live history data unavailable",
            "points": [],
        }
    )

    revenue_expenses = (
        get_revenue_expenses(symbol, frequency="quarterly")
        if has_live_price
        else {
            "symbol": symbol,
            "points": [],
            "currency": profile.get("currency") or quote.get("currency") or "",
        }
    )

    return {
        "symbol": symbol,
        "quote": {
            "symbol": quote.get("symbol", symbol),
            "name": quote.get("name"),
            "exchange": quote.get("exchange"),
            "market": quote.get("market"),
            "currency": quote.get("currency"),
            "price": quote.get("price"),
            "change": quote.get("change"),
            "changePercent": quote.get("changePercent"),
            "marketState": quote.get("marketState"),
            "asOf": quote.get("asOf"),
        },
        "ratios": {
            "symbol": ratios.get("symbol", symbol),
            "market_cap": ratios.get("market_cap"),
            "pe_ttm": ratios.get("pe_ttm"),
            "pb": ratios.get("pb"),
            "ps_ttm": ratios.get("ps_ttm"),
            "dividend_yield": ratios.get("dividend_yield"),
            "beta": ratios.get("beta"),
            "profit_margin": ratios.get("profit_margin"),
            "operating_margin": ratios.get("operating_margin"),
            "roe": ratios.get("roe"),
            "roa": ratios.get("roa"),
            "debt_to_equity": ratios.get("debt_to_equity"),
            "current_ratio": ratios.get("current_ratio"),
            "quick_ratio": ratios.get("quick_ratio"),
        },
        "sector": profile.get("sector"),
        "industry": profile.get("industry"),
        "market_cap": profile.get("market_cap") or ratios.get("market_cap"),
        "currency": profile.get("currency") or quote.get("currency"),
        "summary": profile.get("summary"),
        "recommendation": recommendation,
        "forecast": {
            "symbol": forecast.get("symbol", symbol),
            "model": forecast.get("model", "ARIMA"),
            "interval": forecast.get("interval", "1d"),
            "horizon_days": forecast.get("horizon_days", 30),
            "last_close": forecast.get("last_close"),
            "forecast_close": forecast.get("forecast_close"),
            "error": forecast.get("error"),
            "points": [
                {"date": p.get("t"), "value": p.get("y")}
                for p in forecast.get("points", [])
            ],
        },
        "revenue_expenses": {
            "symbol": revenue_expenses.get("symbol", symbol),
            "points": [
                {
                    "date": p.get("period"),
                    "revenue": p.get("revenue"),
                    "expenses": p.get("expenses"),
                }
                for p in revenue_expenses.get("points", [])
            ],
            "currency": revenue_expenses.get("currency"),
        },
    }
