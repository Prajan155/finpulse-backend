from __future__ import annotations
from statistics import mean

from app.services.yahoo_service import get_company_profile
from app.services.sector_peers import SECTOR_PEERS, INDIA_SECTOR_PEERS
from typing import Dict, List, Tuple
import pandas as pd
from app.core.cache import ttl_cache

from app.services.yahoo_service import (
    get_quote,
    get_ratios,
    get_history,
    get_revenue_expenses,
    get_long_range_forecast_arima,
)

def _is_indian_symbol(symbol: str) -> bool:
    return symbol.endswith(".NS") or symbol.endswith(".BO")


def _get_sector_peer_list(symbol: str, sector: str) -> List[str]:
    if not sector:
        return []

    if _is_indian_symbol(symbol):
        peers = INDIA_SECTOR_PEERS.get(sector, [])
    else:
        peers = SECTOR_PEERS.get(sector, [])

    return [p for p in peers if p != symbol][:4]


def _get_peer_metrics(peer_symbols: List[str]) -> Dict[str, float]:
    margins = []
    roes = []
    debts = []

    for peer in peer_symbols:
        try:
            peer_ratios = get_ratios(peer)

            pm = _safe_float(peer_ratios.get("profit_margin"), default=None)
            roe = _safe_float(peer_ratios.get("return_on_equity"), default=None)
            dte = _safe_float(peer_ratios.get("debt_to_equity"), default=None)

            if pm is not None:
                margins.append(pm)
            if roe is not None:
                roes.append(roe)
            if dte is not None and dte > 0:
                debts.append(dte)
        except Exception:
            continue

    return {
        "avg_profit_margin": mean(margins) if margins else 0.0,
        "avg_roe": mean(roes) if roes else 0.0,
        "avg_debt_to_equity": mean(debts) if debts else 0.0,
    }

def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _label_from_score(score: float) -> str:
    if score >= 8.0:
        return "STRONG BUY"
    if score >= 6.2:
        return "BUY"
    if score >= 4.5:
        return "HOLD"
    if score >= 2.5:
        return "SELL"
    return "STRONG SELL"


def _unique_reasons(items: List[str], limit: int = 6) -> List[str]:
    seen = set()
    out = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            out.append(item)
        if len(out) >= limit:
            break
    return out


def _compute_price_regime_features(closes: List[float]) -> Dict[str, float]:
    if len(closes) < 50:
        return {
            "momentum_20": 0.0,
            "momentum_60": 0.0,
            "volatility_20": 0.0,
            "trend_strength": 0.0,
        }

    s = pd.Series(closes, dtype="float64")

    last_close = float(s.iloc[-1])
    sma20 = float(s.rolling(20).mean().iloc[-1])
    sma50 = float(s.rolling(50).mean().iloc[-1])

    momentum_20 = ((last_close / float(s.iloc[-20])) - 1.0) if float(s.iloc[-20]) else 0.0
    momentum_60 = ((last_close / float(s.iloc[-60])) - 1.0) if len(s) >= 60 and float(s.iloc[-60]) else 0.0

    returns = s.pct_change().dropna()
    volatility_20 = float(returns.tail(20).std()) if len(returns) >= 20 else 0.0
    trend_strength = ((sma20 - sma50) / sma50) if sma50 else 0.0

    return {
        "momentum_20": momentum_20,
        "momentum_60": momentum_60,
        "volatility_20": volatility_20,
        "trend_strength": trend_strength,
    }


def _detect_regime(
    closes: List[float],
    revenue_growth: float,
    profit_margin: float,
    roe: float,
) -> Tuple[str, Dict[str, float]]:
    feats = _compute_price_regime_features(closes)

    momentum_20 = feats["momentum_20"]
    momentum_60 = feats["momentum_60"]
    volatility_20 = feats["volatility_20"]
    trend_strength = feats["trend_strength"]

    # High-volatility / momentum regime
    if volatility_20 >= 0.035 and (abs(momentum_20) >= 0.08 or abs(momentum_60) >= 0.12):
        regime = "high_volatility"

    # Growth regime
    elif (
        revenue_growth >= 0.08
        and (momentum_20 > 0.03 or trend_strength > 0.02)
    ):
        regime = "growth"

    # Quality / stable regime
    elif (
        profit_margin >= 0.10
        and roe >= 0.12
        and volatility_20 < 0.025
    ):
        regime = "quality"

    # Weak / defensive regime
    elif momentum_20 < -0.05 and trend_strength < -0.02:
        regime = "defensive"

    else:
        regime = "balanced"

    return regime, feats


def _get_regime_weights(regime: str) -> Dict[str, float]:
    # Weights sum to 1.0
    weights = {
        "growth": {
            "technical": 0.32,
            "fundamental": 0.22,
            "growth": 0.23,
            "forecast": 0.23,
        },
        "quality": {
            "technical": 0.20,
            "fundamental": 0.40,
            "growth": 0.20,
            "forecast": 0.20,
        },
        "high_volatility": {
            "technical": 0.34,
            "fundamental": 0.14,
            "growth": 0.14,
            "forecast": 0.38,
        },
        "defensive": {
            "technical": 0.18,
            "fundamental": 0.42,
            "growth": 0.16,
            "forecast": 0.24,
        },
        "balanced": {
            "technical": 0.25,
            "fundamental": 0.30,
            "growth": 0.20,
            "forecast": 0.25,
        },
    }
    return weights.get(regime, weights["balanced"])

@ttl_cache(prefix="stock_recommendation", ttl_seconds=300)
def get_stock_recommendation(symbol: str) -> Dict:
    symbol = symbol.upper().strip()

    quote = get_quote(symbol)
    ratios = get_ratios(symbol)
    history = get_history(symbol, interval="1d", range_="6mo")
    revenue = get_revenue_expenses(symbol, frequency="quarterly")
    forecast = get_long_range_forecast_arima(symbol, horizon_days=30, interval="1d")
    profile = get_company_profile(symbol)
    sector = profile.get("sector", "")

    reasons: List[str] = []
    signals: Dict[str, str] = {}

    data_coverage = 0
    data_total = 5

    current_price = _safe_float(quote.get("price"))
    if current_price <= 0:
        current_price = _safe_float(forecast.get("last_close"))

    score = 5.0

    # -------------------------
    # Technical
    # -------------------------
    candles = history.get("candles", []) or []
    closes = [float(c.get("c")) for c in candles if c.get("c") is not None]
    volatility_20 = 0.0
    trend_strength = 0.0

    technical_score = 0.0

    if len(closes) >= 50:
        data_coverage += 1

        s = pd.Series(closes, dtype="float64")
        returns = s.pct_change().dropna()
        volatility_20 = float(returns.tail(20).std()) if len(returns) >= 20 else 0.0
        last_close = float(s.iloc[-1])
        sma20 = float(s.rolling(20).mean().iloc[-1])
        sma50 = float(s.rolling(50).mean().iloc[-1])

        price_vs_sma20 = ((last_close - sma20) / sma20) if sma20 else 0.0
        price_vs_sma50 = ((last_close - sma50) / sma50) if sma50 else 0.0
        sma_spread = ((sma20 - sma50) / sma50) if sma50 else 0.0

        technical_score += _clamp(price_vs_sma20 * 12, -1.0, 1.0)
        technical_score += _clamp(price_vs_sma50 * 14, -1.2, 1.2)
        technical_score += _clamp(sma_spread * 18, -1.3, 1.3)
        trend_strength = sma_spread

        reasons.append("Price is above 20-day average" if price_vs_sma20 > 0 else "Price is below 20-day average")
        reasons.append("Price is above 50-day average" if price_vs_sma50 > 0 else "Price is below 50-day average")
        reasons.append(
            "Short-term trend is stronger than medium-term trend"
            if sma_spread > 0
            else "Short-term trend is weaker than medium-term trend"
        )
    else:
        reasons.append("Not enough price history for a strong technical signal")

    technical_score = _clamp(technical_score, -3.0, 3.0)

    if technical_score >= 1.0:
        signals["technical"] = "bullish"
    elif technical_score <= -1.0:
        signals["technical"] = "bearish"
    else:
        signals["technical"] = "neutral"

    # -------------------------
    # Fundamentals
    # -------------------------
    current_ratio = _safe_float(ratios.get("current_ratio"))
    debt_to_equity = _safe_float(ratios.get("debt_to_equity"))
    profit_margin = _safe_float(ratios.get("profit_margin"))
    roe = _safe_float(ratios.get("roe"))

    if any([
        current_ratio > 0,
        debt_to_equity > 0,
        abs(profit_margin) > 0,
        abs(roe) > 0,
    ]):
        data_coverage += 1

    fundamental_score = 0.0

    fundamental_score += _clamp(profit_margin * 8, -1.5, 1.5)
    fundamental_score += _clamp(roe * 7, -1.2, 1.5)

    if debt_to_equity > 0:
        fundamental_score += _clamp((1.5 - debt_to_equity) * 0.8, -1.5, 1.0)

    if current_ratio > 0:
        fundamental_score += _clamp((current_ratio - 1.0) * 0.6, -0.8, 1.0)

    if profit_margin > 0.10:
        reasons.append("Profit margin is healthy")
    elif profit_margin < 0:
        reasons.append("Company is currently unprofitable")

    if roe > 0.12:
        reasons.append("Return on equity is strong")
    elif 0 < roe < 0.05:
        reasons.append("Return on equity is weak")

    if debt_to_equity > 2.5:
        reasons.append("Debt-to-equity is elevated")
    elif 0 < debt_to_equity <= 1.5:
        reasons.append("Debt-to-equity looks manageable")

    fundamental_score = _clamp(fundamental_score, -3.0, 3.0)

    if fundamental_score >= 1.0:
        signals["fundamental"] = "bullish"
    elif fundamental_score <= -1.0:
        signals["fundamental"] = "bearish"
    else:
        signals["fundamental"] = "neutral"

    # -------------------------
    # Sector-relative scoring
    # -------------------------
    sector_score = 0.0

    peer_symbols = _get_sector_peer_list(symbol, sector)
    peer_metrics = _get_peer_metrics(peer_symbols) if peer_symbols else {}

    if peer_symbols and peer_metrics:
        avg_pm = _safe_float(peer_metrics.get("avg_profit_margin"))
        avg_roe = _safe_float(peer_metrics.get("avg_roe"))
        avg_dte = _safe_float(peer_metrics.get("avg_debt_to_equity"))

        if avg_pm:
            pm_gap = profit_margin - avg_pm
            sector_score += _clamp(pm_gap * 10, -0.8, 0.8)
            if pm_gap > 0.02:
                reasons.append("Profit margin is above sector peers")
            elif pm_gap < -0.02:
                reasons.append("Profit margin is below sector peers")

        if avg_roe:
            roe_gap = roe - avg_roe
            sector_score += _clamp(roe_gap * 8, -0.8, 0.8)
            if roe_gap > 0.03:
                reasons.append("Return on equity is above sector peers")
            elif roe_gap < -0.03:
                reasons.append("Return on equity is below sector peers")

        if avg_dte > 0 and debt_to_equity > 0:
            dte_gap = avg_dte - debt_to_equity
            sector_score += _clamp(dte_gap * 0.4, -0.7, 0.7)
            if dte_gap > 0.3:
                reasons.append("Leverage is better than sector peers")
            elif dte_gap < -0.3:
                reasons.append("Leverage is weaker than sector peers")

    sector_score = _clamp(sector_score, -1.5, 1.5)

    if sector_score >= 0.5:
        signals["sector"] = "bullish"
    elif sector_score <= -0.5:
        signals["sector"] = "bearish"
    else:
        signals["sector"] = "neutral"

    # -------------------------
    # Revenue growth
    # -------------------------
    growth_score = 0.0
    rev_points = revenue.get("points", []) or []

    valid_revenue = []
    for p in rev_points:
        rev = p.get("revenue")
        if rev is not None:
            valid_revenue.append(_safe_float(rev))

    revenue_growth = 0.0
    if len(valid_revenue) >= 2:
        data_coverage += 1

        latest_rev = valid_revenue[-1]
        prev_rev = valid_revenue[-2]

        if prev_rev > 0:
            revenue_growth = (latest_rev - prev_rev) / prev_rev
            growth_score += _clamp(revenue_growth * 8, -2.0, 2.0)
            reasons.append("Revenue is growing" if revenue_growth > 0 else "Revenue is declining")
    else:
        reasons.append("Not enough revenue history for a strong growth signal")

    growth_score = _clamp(growth_score, -2.0, 2.0)

    if growth_score >= 0.8:
        signals["growth"] = "bullish"
    elif growth_score <= -0.8:
        signals["growth"] = "bearish"
    else:
        signals["growth"] = "neutral"

    # -------------------------
    # Forecast
    # -------------------------
    target_price = _safe_float(forecast.get("forecast_close"), current_price)
    if target_price <= 0:
        target_price = current_price

    upside_percent = 0.0
    forecast_score = 0.0

    if current_price > 0 and target_price > 0:
        data_coverage += 1

        upside_percent = ((target_price - current_price) / current_price) * 100.0
        forecast_score += _clamp(upside_percent / 5.0, -2.0, 2.0)

        if upside_percent > 0:
            reasons.append("Forecast implies upside from current levels")
        elif upside_percent < 0:
            reasons.append("Forecast implies downside from current levels")
        # Risk-adjusted downside penalty
        if upside_percent < 0:
            downside_strength = abs(upside_percent) / 14.0
            volatility_penalty = _clamp(volatility_20 * 14, 0.0, 0.9)
            trend_penalty = 0.4 if trend_strength < 0 else 0.0

            downside_penalty = _clamp(
                downside_strength + volatility_penalty + trend_penalty,
                0.0,
                2.0,
            )

            forecast_score -= downside_penalty
            reasons.append("Downside risk is amplified by volatility/trend weakness")
        else:
            reasons.append("Forecast is close to current price")

    forecast_score = _clamp(forecast_score, -2.0, 2.0)

    if forecast_score >= 0.8:
        signals["forecast"] = "bullish"
    elif forecast_score <= -0.8:
        signals["forecast"] = "bearish"
    else:
        signals["forecast"] = "neutral"

    # -------------------------
    # Regime-aware weighting
    # -------------------------
    regime, regime_features = _detect_regime(
        closes=closes,
        revenue_growth=revenue_growth,
        profit_margin=profit_margin,
        roe=roe,
    )

    weights = _get_regime_weights(regime)

    weighted_adjustment = (
        technical_score * weights["technical"]
        + (fundamental_score + sector_score) * weights["fundamental"]
        + growth_score * weights["growth"]
        + forecast_score * weights["forecast"]
    )

    # Scale weighted composite back to score space
    score += weighted_adjustment * 2.0

    # Regime-specific reason
    if regime == "growth":
        reasons.append("Stock is behaving like a growth-driven name")
    elif regime == "quality":
        reasons.append("Stock is behaving like a stable quality name")
    elif regime == "high_volatility":
        reasons.append("Stock is in a high-volatility regime")
    elif regime == "defensive":
        reasons.append("Stock is in a weak or defensive regime")
    else:
        reasons.append("Stock is in a balanced regime")

    # -------------------------
    # Confidence
    # -------------------------
    weighted_components = [
        technical_score * weights["technical"],
        (fundamental_score + sector_score) * weights["fundamental"],
        growth_score * weights["growth"],
        forecast_score * weights["forecast"],
    ]

    strength = sum(abs(x) for x in weighted_components) / 3.0

    non_zero = [x for x in weighted_components if abs(x) >= 0.20]
    if non_zero:
        pos = sum(1 for x in non_zero if x > 0)
        neg = sum(1 for x in non_zero if x < 0)
        agreement = abs(pos - neg) / len(non_zero)
    else:
        agreement = 0.3

    confidence = _clamp(0.40 + (0.30 * strength) + (0.20 * agreement), 0.40, 0.95)

    coverage_ratio = data_coverage / data_total
    confidence = confidence * (0.65 + 0.35 * coverage_ratio)

    # Slight confidence haircut for volatile regimes
    if regime == "high_volatility":
        confidence *= 0.92

    confidence = _clamp(confidence, 0.35, 0.95)

    score = _clamp(score, 0.0, 10.0)
    score = round(score, 2)
    confidence = round(confidence, 2)

    recommendation = _label_from_score(score)

    return {
        "symbol": symbol,
        "regime": regime,
        "sector": sector or "Unknown",
        "recommendation": recommendation,
        "score": score,
        "confidence": confidence,
        "current_price": round(current_price, 2),
        "target_price": round(target_price, 2),
        "upside_percent": round(upside_percent, 2),
        "signals": signals,
        "reasons": _unique_reasons(reasons, limit=6),
    }