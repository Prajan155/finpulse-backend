from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.api.auth import router as auth_router
from app.api.portfolio_storage import router as portfolio_storage_router
from app.api.watchlist import router as watchlist_router
from app.core.cache import cache
from app.core.config import settings
from app.core.dependencies import get_db
from app.models.stock import Stock
from app.schemas.compare import CompareResponseOut
from app.schemas.portfolio import (
    PortfolioOverviewIn,
    PortfolioOverviewOut,
    PortfolioPerformanceOut,
    PortfolioRecommendationIn,
    PortfolioRecommendationOut,
)
from app.schemas.stock import (
    BatchQuotesIn,
    CorrelationOut,
    ForecastOut,
    HistoryOut,
    HistoryPoint,
    QuoteOut,
    RatiosOut,
    RecommendationOut,
    RevenueExpensesOut,
    StockOverviewOut,
    SymbolSearchOut,
)
from app.schemas.stocks_db import StockCreate, StockOut
from app.services.chart_service import get_chart_data
from app.services.compare_service import get_compare_data
from app.services.indicators_service import get_indicator_data
from app.services.overview_service import get_stock_overview
from app.services.portfolio_performance_service import get_portfolio_performance
from app.services.portfolio_service import (
    get_portfolio_overview,
    get_portfolio_recommendation,
)
from app.services.prices_service import get_price_history, refresh_price_history
from app.services.recommendation_service import get_stock_recommendation
from app.services.yahoo_service import (
    get_correlation_matrix,
    get_history,
    get_intraday_forecast_arima,
    get_long_range_forecast_arima,
    get_quote,
    get_quotes_batch,
    get_ratios,
    search_symbols,
)

router = APIRouter()

router.include_router(auth_router)
router.include_router(watchlist_router)
router.include_router(portfolio_storage_router)


def _safe_num(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


@router.get("/symbols/search", response_model=SymbolSearchOut)
def symbols_search(q: str):
    q = q.strip()
    if not q:
        raise HTTPException(status_code=400, detail="Query cannot be empty")

    result = search_symbols(q)
    return {
        "query": q,
        "results": result.get("results", []),
    }


@router.get("/stocks/compare", response_model=CompareResponseOut)
def compare_stocks(
    symbols: str = Query(..., description="Comma separated stock symbols"),
    range: str = Query("6mo", description="Comparison history range"),
):
    allowed_ranges = {"1mo", "3mo", "6mo", "1y", "2y", "5y"}
    if range not in allowed_ranges:
        raise HTTPException(status_code=400, detail="invalid range")

    parsed_symbols: List[str] = [
        item.strip().upper() for item in symbols.split(",") if item.strip()
    ]

    if len(parsed_symbols) < 2:
        raise HTTPException(
            status_code=400, detail="Please provide at least 2 stock symbols."
        )

    if len(parsed_symbols) > 4:
        raise HTTPException(
            status_code=400, detail="You can compare up to 4 stock symbols only."
        )

    return get_compare_data(parsed_symbols, range_=range)


@router.get("/stocks/{symbol}/history", response_model=HistoryOut)
def history(symbol: str, interval: str = "1d", range: str = "6mo"):
    allowed_intervals = {"5m", "15m", "30m", "60m", "1d", "1wk", "1mo"}
    allowed_ranges = {"1mo", "3mo", "6mo", "1y", "2y", "5y"}

    if interval not in allowed_intervals:
        raise HTTPException(status_code=400, detail="invalid interval")
    if range not in allowed_ranges:
        raise HTTPException(status_code=400, detail="invalid range")

    result = get_history(symbol, interval=interval, range_=range)

    return {
        "symbol": result.get("symbol", symbol.upper()),
        "points": [
            {
                "date": c.get("t"),
                "open": c.get("o"),
                "high": c.get("h"),
                "low": c.get("l"),
                "close": c.get("c"),
                "volume": c.get("v"),
            }
            for c in result.get("candles", [])
        ],
    }


@router.get("/stocks/{symbol}/quote", response_model=QuoteOut)
def quote(symbol: str):
    result = get_quote(symbol)

    return {
        "symbol": result.get("symbol", symbol.upper()),
        "name": result.get("name"),
        "exchange": result.get("exchange"),
        "market": result.get("market"),
        "currency": result.get("currency"),
        "price": result.get("price"),
        "change": result.get("change"),
        "changePercent": result.get("changePercent"),
        "marketState": result.get("marketState"),
        "asOf": result.get("asOf"),
    }


@router.get("/stocks/{symbol}/ratios", response_model=RatiosOut)
def ratios(symbol: str):
    result = get_ratios(symbol)

    return {
        "symbol": result.get("symbol", symbol.upper()),
        "market_cap": result.get("market_cap"),
        "pe_ttm": result.get("pe_ttm"),
        "pb": result.get("pb"),
        "ps_ttm": result.get("ps_ttm"),
        "dividend_yield": result.get("dividend_yield"),
        "beta": result.get("beta"),
        "profit_margin": result.get("profit_margin"),
        "operating_margin": result.get("operating_margin"),
        "roe": result.get("roe"),
        "roa": result.get("roa"),
        "debt_to_equity": result.get("debt_to_equity"),
        "current_ratio": result.get("current_ratio"),
        "quick_ratio": result.get("quick_ratio"),
    }


@router.get("/stocks/{symbol}/forecast/intraday", response_model=ForecastOut)
def forecast_intraday(symbol: str, horizon_days: int = 3, interval: str = "15m"):
    if interval not in {"5m", "15m"}:
        raise HTTPException(status_code=400, detail="interval must be 5m or 15m")

    max_days = 2 if interval == "5m" else 5
    if horizon_days > max_days:
        raise HTTPException(
            status_code=400,
            detail=f"Maximum horizon for {interval} forecasts is {max_days} trading days",
        )

    result = get_intraday_forecast_arima(
        symbol,
        horizon_days=horizon_days,
        interval=interval,
    )

    return {
        "symbol": result.get("symbol", symbol.upper()),
        "model": result.get("model", "ARIMA"),
        "interval": result.get("interval", interval),
        "horizon_days": result.get("horizon_days", horizon_days),
        "last_close": result.get("last_close"),
        "forecast_close": result.get("forecast_close"),
        "error": result.get("error"),
        "points": [
            {
                "date": p.get("t"),
                "value": p.get("y"),
            }
            for p in result.get("points", [])
        ],
    }


@router.get("/stocks/{symbol}/forecast/long-range", response_model=ForecastOut)
def forecast_long_range(symbol: str, horizon_days: int = 30, interval: str = "1d"):
    if interval not in {"1d"}:
        raise HTTPException(
            status_code=400,
            detail="long-range forecast currently supports only 1d interval",
        )

    if horizon_days > 365:
        raise HTTPException(
            status_code=400,
            detail="Maximum horizon for long-range forecast is 365 days",
        )

    result = get_long_range_forecast_arima(
        symbol,
        horizon_days=horizon_days,
        interval=interval,
    )

    return {
        "symbol": result.get("symbol", symbol.upper()),
        "model": result.get("model", "ARIMA"),
        "interval": result.get("interval", interval),
        "horizon_days": result.get("horizon_days", horizon_days),
        "last_close": result.get("last_close"),
        "forecast_close": result.get("forecast_close"),
        "error": result.get("error"),
        "points": [
            {
                "date": p.get("t"),
                "value": p.get("y"),
            }
            for p in result.get("points", [])
        ],
    }


@router.get("/correlation", response_model=CorrelationOut)
def correlation(
    tickers: str = Query(..., description="Comma-separated stock tickers"),
    range: str = "6mo",
):
    allowed_ranges = {"1mo", "3mo", "6mo", "1y", "2y", "5y"}
    if range not in allowed_ranges:
        raise HTTPException(status_code=400, detail="invalid range")

    ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]

    if len(ticker_list) < 2:
        raise HTTPException(status_code=400, detail="Please provide at least 2 tickers")

    result = get_correlation_matrix(ticker_list, range_=range)

    range_map = {
        "1mo": 21,
        "3mo": 63,
        "6mo": 126,
        "1y": 252,
        "2y": 504,
        "5y": 1260,
    }

    return {
        "symbols": result.get("tickers", ticker_list),
        "window_days": range_map.get(range, 126),
        "matrix": result.get("matrix", []),
    }


@router.get("/stocks/{symbol}/revenue-expenses", response_model=RevenueExpensesOut)
def revenue_expenses(symbol: str, frequency: str = "quarterly"):
    import yfinance as yf

    ticker = yf.Ticker(symbol)

    try:
        financials = (
            ticker.quarterly_financials
            if frequency == "quarterly"
            else ticker.financials
        )

        if financials is None or financials.empty:
            return {
                "symbol": symbol.upper(),
                "frequency": frequency,
                "points": [],
            }

        points = []

        revenue_row = (
            financials.loc["Total Revenue"]
            if "Total Revenue" in financials.index
            else None
        )
        expenses_row = (
            financials.loc["Total Expenses"]
            if "Total Expenses" in financials.index
            else None
        )
        gross_profit_row = (
            financials.loc["Gross Profit"]
            if "Gross Profit" in financials.index
            else None
        )

        for date in financials.columns:
            revenue = revenue_row.get(date) if revenue_row is not None else None
            expenses = expenses_row.get(date) if expenses_row is not None else None

            if expenses is None and revenue is not None and gross_profit_row is not None:
                gross_profit = gross_profit_row.get(date)
                if gross_profit is not None:
                    expenses = revenue - gross_profit

            points.append(
                {
                    "date": str(date.date()),
                    "revenue": float(revenue) if revenue is not None else 0,
                    "expenses": float(expenses) if expenses is not None else 0,
                }
            )

        return {
            "symbol": symbol.upper(),
            "frequency": frequency,
            "points": sorted(points, key=lambda x: x["date"]),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stocks", response_model=list[StockOut])
def list_stocks(db: Session = Depends(get_db)):
    return db.query(Stock).all()


@router.get("/prices/{symbol}", response_model=HistoryOut)
def prices(symbol: str, limit: int = 252, db: Session = Depends(get_db)):
    rows = get_price_history(db, symbol=symbol, limit=limit)
    return {
        "symbol": symbol.upper(),
        "points": [
            HistoryPoint(
                date=r.date,
                open=r.open,
                high=r.high,
                low=r.low,
                close=r.close,
                volume=r.volume,
            )
            for r in rows
        ],
    }


@router.get("/indicators/{symbol}")
def indicators(symbol: str, db: Session = Depends(get_db)):
    data = get_indicator_data(db, symbol)
    return {"symbol": symbol.upper(), "data": data}


@router.get("/debug/prices_count/{symbol}")
def debug_prices_count(symbol: str, db: Session = Depends(get_db)):
    symbol = symbol.upper().strip()
    count = db.execute(
        text("SELECT COUNT(*) FROM price_history WHERE symbol = :s"),
        {"s": symbol},
    ).scalar()
    return {"symbol": symbol, "count": int(count or 0)}


@router.get("/chart/{symbol}")
def chart(symbol: str, db: Session = Depends(get_db)):
    data = get_chart_data(db, symbol)
    return {"symbol": symbol.upper(), "data": data}


@router.post("/quotes", response_model=list[QuoteOut])
def batch_quotes(payload: BatchQuotesIn):
    tickers = [t.strip().upper() for t in payload.symbols if t.strip()]
    if not tickers:
        raise HTTPException(status_code=400, detail="symbols list is empty")
    if len(tickers) > settings.max_tickers_per_batch:
        raise HTTPException(status_code=400, detail="too many tickers in one request")
    return get_quotes_batch(tickers)


@router.post("/stocks", response_model=StockOut)
def add_stock(payload: StockCreate, db: Session = Depends(get_db)):
    symbol = payload.symbol.upper().strip()

    existing = db.get(Stock, symbol)
    if existing:
        return existing

    stock = Stock(
        symbol=symbol,
        name=payload.name,
        exchange=payload.exchange,
        market=payload.market,
    )

    db.add(stock)
    db.commit()
    db.refresh(stock)

    return stock


@router.get("/stocks/{symbol}/recommendation", response_model=RecommendationOut)
def stock_recommendation(symbol: str):
    return get_stock_recommendation(symbol)


@router.get("/stocks/{symbol}/overview", response_model=StockOverviewOut)
def stock_overview(symbol: str):
    return get_stock_overview(symbol)


@router.post("/portfolio/recommendation", response_model=PortfolioRecommendationOut)
def portfolio_recommendation(payload: PortfolioRecommendationIn):
    positions = [
        {
            "symbol": p.symbol,
            "weight": _safe_num(p.weight, 0.0),
        }
        for p in payload.positions
    ]
    return get_portfolio_recommendation(positions)


@router.post("/portfolio/overview", response_model=PortfolioOverviewOut)
def portfolio_overview(payload: PortfolioOverviewIn):
    positions = []

    total_value = sum(
        _safe_num(p.shares, 0.0) * _safe_num(p.avg_buy_price, 0.0)
        for p in payload.positions
    )

    for p in payload.positions:
        shares = _safe_num(p.shares, 0.0)
        avg_buy_price = _safe_num(p.avg_buy_price, 0.0)
        value = shares * avg_buy_price
        weight = (value / total_value) if total_value > 0 else 0.0

        positions.append(
            {
                "symbol": p.symbol,
                "weight": weight,
                "shares": shares,
                "avg_buy_price": avg_buy_price,
            }
        )

    return get_portfolio_overview(positions)


@router.post("/portfolio/performance", response_model=PortfolioPerformanceOut)
def portfolio_performance(payload: PortfolioOverviewIn):
    positions = []

    total_value = sum(
        _safe_num(p.shares, 0.0) * _safe_num(p.avg_buy_price, 0.0)
        for p in payload.positions
    )

    for p in payload.positions:
        shares = _safe_num(p.shares, 0.0)
        avg_buy_price = _safe_num(p.avg_buy_price, 0.0)
        value = shares * avg_buy_price
        weight = (value / total_value) if total_value > 0 else 0.0

        positions.append(
            {
                "symbol": p.symbol,
                "weight": weight,
                "shares": shares,
                "avg_buy_price": avg_buy_price,
            }
        )

    return get_portfolio_performance(positions)


@router.post("/prices/{symbol}/refresh")
def refresh_prices(symbol: str, db: Session = Depends(get_db)):
    inserted = refresh_price_history(db, symbol=symbol, period="1y", interval="1d")
    cache.clear()
    return {"symbol": symbol.upper(), "inserted": inserted}


@router.post("/debug/cache/clear")
def clear_app_cache():
    cache.clear()
    return {"ok": True, "message": "Cache cleared"}