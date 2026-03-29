from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

from app.core.cache import ttl_cache
from app.services.recommendation_service import get_stock_recommendation
from app.services.yahoo_service import get_company_profile, get_correlation_matrix


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _safe_symbol(value) -> str:
    return str(value or "").upper().strip()


def _normalize_weights(positions: List[dict]) -> List[dict]:
    """
    Accepts either:
    - explicit weights
    - shares/quantity + avg_buy_price/average_price
    Merges duplicate symbols and derives weights when missing.
    """
    merged: Dict[str, dict] = {}

    for p in positions or []:
        symbol = _safe_symbol(p.get("symbol"))
        if not symbol:
            continue

        shares = _safe_float(
            p.get("shares", p.get("quantity")),
            0.0,
        )
        avg_buy_price = _safe_float(
            p.get("avg_buy_price", p.get("average_price")),
            0.0,
        )
        explicit_weight = _safe_float(p.get("weight"), 0.0)

        if symbol not in merged:
            merged[symbol] = {
                "symbol": symbol,
                "weight": 0.0,
                "shares": 0.0,
                "avg_buy_price": 0.0,
                "invested_value": 0.0,
            }

        item = merged[symbol]

        previous_shares = item["shares"]
        previous_invested = item["invested_value"]

        current_invested = shares * avg_buy_price
        total_shares = previous_shares + shares
        total_invested = previous_invested + current_invested

        item["shares"] = total_shares
        item["invested_value"] = total_invested
        item["weight"] += explicit_weight

        if total_shares > 0:
            item["avg_buy_price"] = total_invested / total_shares
        elif avg_buy_price > 0:
            item["avg_buy_price"] = avg_buy_price

    cleaned = list(merged.values())
    if not cleaned:
        return []

    total_explicit_weight = sum(max(0.0, _safe_float(p.get("weight"), 0.0)) for p in cleaned)

    if total_explicit_weight > 0:
        return [
            {
                "symbol": p["symbol"],
                "weight": _safe_float(p["weight"], 0.0) / total_explicit_weight,
                "shares": _safe_float(p.get("shares"), 0.0),
                "avg_buy_price": _safe_float(p.get("avg_buy_price"), 0.0),
            }
            for p in cleaned
        ]

    total_invested = sum(max(0.0, _safe_float(p.get("invested_value"), 0.0)) for p in cleaned)
    if total_invested > 0:
        return [
            {
                "symbol": p["symbol"],
                "weight": _safe_float(p.get("invested_value"), 0.0) / total_invested,
                "shares": _safe_float(p.get("shares"), 0.0),
                "avg_buy_price": _safe_float(p.get("avg_buy_price"), 0.0),
            }
            for p in cleaned
        ]

    equal_weight = 1.0 / len(cleaned)
    return [
        {
            "symbol": p["symbol"],
            "weight": equal_weight,
            "shares": _safe_float(p.get("shares"), 0.0),
            "avg_buy_price": _safe_float(p.get("avg_buy_price"), 0.0),
        }
        for p in cleaned
    ]


def _avg_offdiag_corr(matrix: List[List[float]]) -> float:
    vals = []
    n = len(matrix)

    for i in range(n):
        for j in range(n):
            if i < j:
                try:
                    vals.append(float(matrix[i][j]))
                except Exception:
                    continue

    return sum(vals) / len(vals) if vals else 0.0


def _risk_level_from_score(risk_score: float) -> str:
    if risk_score >= 0.72:
        return "high"
    if risk_score >= 0.45:
        return "moderate"
    return "low"


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


def _action_from_score(score: float, current_weight: float, target_weight: float) -> str:
    diff = target_weight - current_weight

    if score >= 8.0 and diff > 0.03:
        return "BUY"
    if score >= 6.2 and diff > 0.015:
        return "ADD"
    if score <= 2.5 and diff < -0.03:
        return "SELL"
    if score <= 4.5 and diff < -0.015:
        return "TRIM"
    return "HOLD"


def _dedupe_list(values: List[str], limit: int = 6) -> List[str]:
    seen = set()
    out = []
    for item in values:
        if item and item not in seen:
            seen.add(item)
            out.append(item)
        if len(out) >= limit:
            break
    return out


def _build_ai_summary(
    *,
    holdings: List[dict],
    sector_concentration: Dict[str, float],
    diversification_score: float,
    concentration_score: float,
    risk_level: str,
    portfolio_score: float,
    total_return_percent: float,
    total_gain_loss: float,
    total_value: float,
    best_holding: dict | None,
    worst_holding: dict | None,
) -> List[str]:
    lines: List[str] = []

    if risk_level:
        lines.append(f"Your portfolio currently reflects {risk_level} risk.")

    if holdings:
        top_holding = max(holdings, key=lambda h: _safe_float(h.get("weight"), 0.0))
        top_holding_weight = _safe_float(top_holding.get("weight"), 0.0)
        if top_holding_weight >= 50:
            lines.append(
                f"{top_holding.get('symbol', 'Top holding')} dominates the portfolio at {top_holding_weight:.2f}%."
            )
        elif top_holding_weight >= 35:
            lines.append(
                f"{top_holding.get('symbol', 'Top holding')} has elevated concentration at {top_holding_weight:.2f}%."
            )

    if sector_concentration:
        top_sector, top_sector_weight = max(
            sector_concentration.items(),
            key=lambda x: x[1],
        )
        if top_sector_weight >= 60:
            lines.append(
                f"{top_sector} is your largest sector exposure at {top_sector_weight:.2f}%, which is quite concentrated."
            )
        elif top_sector_weight >= 40:
            lines.append(
                f"{top_sector} is your largest sector exposure at {top_sector_weight:.2f}%."
            )

    if diversification_score >= 0.65:
        lines.append("Diversification looks reasonably healthy across holdings.")
    elif diversification_score >= 0.40:
        lines.append("Diversification is moderate, with room for better balance.")
    else:
        lines.append("Diversification is weak and concentration risk is elevated.")

    if concentration_score >= 7.0:
        lines.append("Position sizing is fairly balanced overall.")
    elif concentration_score <= 4.0:
        lines.append("Position concentration is high, so a rebalance may improve stability.")

    if total_return_percent > 10:
        lines.append("Portfolio is generating strong positive returns.")
    elif total_return_percent > 0:
        lines.append("Portfolio is slightly profitable but can still be optimized.")
    elif total_return_percent < 0:
        lines.append("Portfolio is currently in a loss, so risk control matters more.")

    if best_holding:
        lines.append(
            f"Best performer: {best_holding['symbol']} at {best_holding['return_percent']:.2f}%."
        )

    if worst_holding and _safe_float(worst_holding.get("return_percent"), 0.0) < 0:
        lines.append(
            f"Weakest performer: {worst_holding['symbol']} at {worst_holding['return_percent']:.2f}%."
        )

    if portfolio_score >= 6.2:
        lines.append("Overall portfolio quality is supported by multiple favorable signals.")
    elif portfolio_score < 4.5:
        lines.append("Overall portfolio quality is under pressure from weaker holdings.")

    if total_value > 0 and total_gain_loss > 0:
        lines.append("The portfolio is currently above invested capital.")

    return _dedupe_list(lines, limit=8)


def _fetch_symbol_bundle(symbol: str) -> tuple[str, dict, dict]:
    rec = get_stock_recommendation(symbol) or {}
    profile = get_company_profile(symbol) or {}
    return symbol, rec, profile


def _build_base_portfolio_context(norm_positions: List[dict]) -> Dict:
    if not norm_positions:
        return {
            "norm_positions": [],
            "symbols": [],
            "stock_recs": {},
            "profiles": {},
            "sector_weights": {},
            "portfolio_score": 0.0,
            "portfolio_confidence": 0.0,
            "diversification_score": 0.0,
            "concentration_score": 0.0,
            "risk_score": 0.0,
            "risk_level": "high",
            "avg_corr": 0.0,
            "warnings": ["Portfolio is empty or weights are invalid"],
            "recommendations": [],
            "holdings": [],
        }

    symbols = [p["symbol"] for p in norm_positions]
    unique_symbols = list(dict.fromkeys(symbols))

    stock_recs: Dict[str, dict] = {}
    profiles: Dict[str, dict] = {}
    sector_weights = defaultdict(float)
    warnings: List[str] = []

    max_workers = min(8, max(1, len(unique_symbols)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_fetch_symbol_bundle, symbol): symbol
            for symbol in unique_symbols
        }
        for future in as_completed(future_map):
            symbol = future_map[future]
            try:
                _, rec, profile = future.result()
            except Exception:
                rec, profile = {}, {}
            stock_recs[symbol] = rec or {}
            profiles[symbol] = profile or {}

    for p in norm_positions:
        symbol = p["symbol"]
        weight = p["weight"]
        profile = profiles.get(symbol, {}) or {}
        sector = profile.get("sector") or "Unknown"
        sector_weights[sector] += weight

    portfolio_score = 0.0
    portfolio_confidence = 0.0

    for p in norm_positions:
        symbol = p["symbol"]
        rec = stock_recs.get(symbol, {}) or {}

        portfolio_score += p["weight"] * _safe_float(rec.get("score"), 0.0)
        portfolio_confidence += p["weight"] * _safe_float(rec.get("confidence"), 0.0)

    portfolio_score = round(_clamp(portfolio_score, 0.0, 10.0), 2)
    portfolio_confidence = round(_clamp(portfolio_confidence, 0.0, 1.0), 2)

    diversification_score = 0.5
    avg_corr = 0.0

    if len(unique_symbols) >= 2:
        try:
            corr = get_correlation_matrix(unique_symbols, range_="6mo")
            matrix = corr.get("matrix", []) or []
            avg_corr = _avg_offdiag_corr(matrix)
            diversification_score = round(_clamp(1.0 - avg_corr, 0.0, 1.0), 2)

            if avg_corr >= 0.75:
                warnings.append("Portfolio holdings are highly correlated")
            elif avg_corr >= 0.55:
                warnings.append("Portfolio has moderate correlation concentration")
        except Exception:
            warnings.append("Could not compute correlation matrix for diversification scoring")

    for sector, wt in sector_weights.items():
        if wt >= 0.60:
            warnings.append(f"{sector} sector exposure exceeds 60%")
        elif wt >= 0.40:
            warnings.append(f"{sector} sector exposure is elevated")

    weighted_vol_risk = 0.0
    weighted_downside_risk = 0.0
    high_vol_count = 0

    for p in norm_positions:
        symbol = p["symbol"]
        weight = p["weight"]
        rec = stock_recs.get(symbol, {}) or {}

        regime = rec.get("regime", "balanced")
        upside_percent = _safe_float(rec.get("upside_percent"), 0.0)
        confidence = _safe_float(rec.get("confidence"), 0.0)
        score = _safe_float(rec.get("score"), 5.0)

        regime_risk = 1.0 if regime == "high_volatility" else 0.6 if regime == "growth" else 0.35
        downside_risk = abs(min(upside_percent, 0.0)) / 15.0
        low_score_risk = max(0.0, (5.0 - score) / 5.0)

        weighted_vol_risk += weight * regime_risk
        weighted_downside_risk += weight * (downside_risk + low_score_risk) * (
            1.1 - min(confidence, 0.95)
        )

        if regime == "high_volatility":
            high_vol_count += 1

    risk_score = _clamp(
        0.45 * weighted_vol_risk
        + 0.35 * weighted_downside_risk
        + 0.20 * max(0.0, avg_corr),
        0.0,
        1.0,
    )
    risk_level = _risk_level_from_score(risk_score)

    if high_vol_count >= max(2, len(unique_symbols) // 2):
        warnings.append("Portfolio contains multiple high-volatility positions")

    weights = [p["weight"] for p in norm_positions]
    hhi = sum(w * w for w in weights)
    concentration_score = round(_clamp(10.0 * (1.0 - hhi), 0.0, 10.0), 2)

    total_positive = 0.0
    raw_targets: Dict[str, float] = {}

    for p in norm_positions:
        symbol = p["symbol"]
        rec = stock_recs.get(symbol, {}) or {}
        score = _safe_float(rec.get("score"), 5.0)
        confidence = _safe_float(rec.get("confidence"), 0.5)

        desirability = max(0.05, (score / 10.0) * (0.6 + 0.4 * confidence))

        sector = (profiles.get(symbol, {}) or {}).get("sector") or "Unknown"
        sector_penalty = 1.0
        if sector_weights[sector] > 0.45:
            sector_penalty = 0.85
        if sector_weights[sector] > 0.60:
            sector_penalty = 0.72

        if avg_corr >= 0.75:
            desirability *= 0.9

        desirability *= sector_penalty
        raw_targets[symbol] = desirability
        total_positive += desirability

    recommendations = []
    holdings = []

    for p in norm_positions:
        symbol = p["symbol"]
        current_weight = p["weight"]
        shares = _safe_float(p.get("shares"), 0.0)
        avg_buy_price = _safe_float(p.get("avg_buy_price"), 0.0)

        rec = stock_recs.get(symbol, {}) or {}
        profile = profiles.get(symbol, {}) or {}

        score = _safe_float(rec.get("score"), 5.0)
        confidence = _safe_float(rec.get("confidence"), 0.5)
        sector = profile.get("sector") or "Unknown"
        regime = rec.get("regime", "balanced")

        current_price = _safe_float(rec.get("current_price"), 0.0)
        target_price = _safe_float(rec.get("target_price"), 0.0)
        upside_percent = _safe_float(rec.get("upside_percent"), 0.0)

        target_weight = (
            raw_targets[symbol] / total_positive if total_positive > 0 else current_weight
        )

        max_shift = 0.12
        target_weight = _clamp(
            target_weight,
            max(0.0, current_weight - max_shift),
            min(1.0, current_weight + max_shift),
        )

        action = _action_from_score(score, current_weight, target_weight)

        reasons = rec.get("reasons", [])
        primary_reason = reasons[0] if reasons else "Based on portfolio score and diversification needs"

        recommendations.append(
            {
                "symbol": symbol,
                "action": action,
                "current_weight": round(current_weight, 4),
                "target_weight": round(target_weight, 4),
                "score": round(score, 2),
                "confidence": round(confidence, 2),
                "reason": primary_reason,
            }
        )

        invested_value = shares * avg_buy_price
        if invested_value <= 0:
            invested_value = shares * current_price

        current_value = shares * current_price
        gain_loss = current_value - invested_value
        return_percent = (gain_loss / invested_value) * 100 if invested_value > 0 else 0.0

        holdings.append(
            {
                "symbol": symbol,
                "shares": shares,
                "avg_buy_price": round(avg_buy_price, 2),
                "current_price": round(current_price, 2),
                "invested_value": round(invested_value, 2),
                "current_value": round(current_value, 2),
                "gain_loss": round(gain_loss, 2),
                "return_percent": round(return_percent, 2),
                "weight": round(current_weight * 100.0, 2),
                "recommendation": rec.get("recommendation", "HOLD"),
                "score": round(score, 2),
                "confidence": round(confidence, 2),
                "target_price": round(target_price, 2),
                "upside_percent": round(upside_percent, 2),
                "sector": sector,
                "regime": regime,
            }
        )

    action_rank = {"SELL": 0, "TRIM": 1, "BUY": 2, "ADD": 3, "HOLD": 4}
    recommendations.sort(
        key=lambda x: (
            action_rank.get(x["action"], 9),
            -abs(x["target_weight"] - x["current_weight"]),
        )
    )

    return {
        "norm_positions": norm_positions,
        "symbols": unique_symbols,
        "stock_recs": stock_recs,
        "profiles": profiles,
        "sector_weights": dict(sector_weights),
        "portfolio_score": portfolio_score,
        "portfolio_confidence": portfolio_confidence,
        "diversification_score": diversification_score,
        "concentration_score": concentration_score,
        "risk_score": round(risk_score, 2),
        "risk_level": risk_level,
        "avg_corr": round(avg_corr, 2),
        "warnings": warnings[:8],
        "recommendations": recommendations,
        "holdings": holdings,
    }


@ttl_cache(prefix="portfolio_recommendation", ttl_seconds=180)
def get_portfolio_recommendation(positions: List[dict]) -> Dict:
    norm_positions = _normalize_weights(positions)
    base = _build_base_portfolio_context(norm_positions)

    if not norm_positions:
        return {
            "portfolio_score": 0.0,
            "diversification_score": 0.0,
            "risk_level": "high",
            "sector_concentration": {},
            "recommendations": [],
            "warnings": ["Portfolio is empty or weights are invalid"],
        }

    return {
        "portfolio_score": base["portfolio_score"],
        "diversification_score": base["diversification_score"],
        "risk_level": base["risk_level"],
        "sector_concentration": {
            k: round(v, 4) for k, v in base["sector_weights"].items()
        },
        "recommendations": base["recommendations"],
        "warnings": base["warnings"],
    }


@ttl_cache(prefix="portfolio_overview", ttl_seconds=180)
def get_portfolio_overview(positions: List[dict]) -> Dict:
    norm_positions = _normalize_weights(positions)
    base = _build_base_portfolio_context(norm_positions)

    if not norm_positions:
        return {
            "total_invested": 0.0,
            "total_value": 0.0,
            "total_gain_loss": 0.0,
            "total_return_percent": 0.0,
            "recommendation": "HOLD",
            "portfolio_score": 0.0,
            "confidence": 0.0,
            "diversification_score": 0.0,
            "concentration_score": 0.0,
            "risk_score": 0.0,
            "risk_level": "high",
            "sector_exposure": {},
            "holdings": [],
            "recommendations": [],
            "top_risks": ["Portfolio is empty or weights are invalid"],
            "top_opportunities": [],
            "ai_summary": [],
            "best_holding": None,
            "worst_holding": None,
            "insight_score": 0.0,
        }

    sector_concentration = {
        sector: round(weight * 100.0, 2)
        for sector, weight in base["sector_weights"].items()
    }

    holdings = base["holdings"]
    top_risks = list(base["warnings"])
    top_opportunities: List[str] = []

    total_value = 0.0
    total_invested = 0.0

    for h in holdings:
        invested_value = _safe_float(h.get("invested_value"), 0.0)
        current_value = _safe_float(h.get("current_value"), 0.0)

        total_value += current_value
        total_invested += invested_value

    total_gain_loss = total_value - total_invested
    total_return_percent = (
        (total_gain_loss / total_invested) * 100 if total_invested > 0 else 0.0
    )

    if holdings:
        max_weight = max(h["weight"] for h in holdings)
        if max_weight > 35:
            top_risks.append("Portfolio is highly concentrated in a single holding")

    if sector_concentration:
        max_sector_name, max_sector_weight = max(
            sector_concentration.items(),
            key=lambda x: x[1],
        )
        if max_sector_weight > 50:
            top_risks.append(f"Sector exposure is heavily concentrated in {max_sector_name}")

    weak_holdings = [h for h in holdings if h["score"] < 4.5]
    if len(weak_holdings) >= max(1, len(holdings) // 3):
        top_risks.append("Several holdings have weak recommendation scores")

    strong_holdings = [h for h in holdings if h["score"] >= 6.2]
    if len(strong_holdings) >= 2:
        top_opportunities.append("Portfolio contains multiple BUY-rated holdings")
    elif len(strong_holdings) == 1:
        top_opportunities.append("Portfolio contains at least one high-quality BUY-rated holding")

    high_upside = [h for h in holdings if h["upside_percent"] >= 8]
    if high_upside:
        top_opportunities.append("Several holdings show strong upside potential")

    if len(sector_concentration) >= 4:
        top_opportunities.append("Sector diversification is healthy")

    if base["diversification_score"] >= 0.65:
        top_opportunities.append("Cross-holding correlation is reasonably contained")

    best_holding = None
    worst_holding = None

    if holdings:
        sorted_by_return = sorted(
            holdings,
            key=lambda h: _safe_float(h.get("return_percent"), 0.0),
        )
        worst_holding = sorted_by_return[0]
        best_holding = sorted_by_return[-1]

    ai_summary = _build_ai_summary(
        holdings=holdings,
        sector_concentration=sector_concentration,
        diversification_score=base["diversification_score"],
        concentration_score=base["concentration_score"],
        risk_level=base["risk_level"],
        portfolio_score=base["portfolio_score"],
        total_return_percent=round(total_return_percent, 2),
        total_gain_loss=round(total_gain_loss, 2),
        total_value=round(total_value, 2),
        best_holding=(
            {
                "symbol": best_holding["symbol"],
                "return_percent": round(_safe_float(best_holding.get("return_percent"), 0.0), 2),
                "gain_loss": round(_safe_float(best_holding.get("gain_loss"), 0.0), 2),
            }
            if best_holding
            else None
        ),
        worst_holding=(
            {
                "symbol": worst_holding["symbol"],
                "return_percent": round(_safe_float(worst_holding.get("return_percent"), 0.0), 2),
                "gain_loss": round(_safe_float(worst_holding.get("gain_loss"), 0.0), 2),
            }
            if worst_holding
            else None
        ),
    )

    insight_score = round(
        _clamp(
            (base["portfolio_score"] * 0.6) + (base["diversification_score"] * 4.0),
            0.0,
            10.0,
        ),
        2,
    )

    return {
        "total_invested": round(total_invested, 2),
        "total_value": round(total_value, 2),
        "total_gain_loss": round(total_gain_loss, 2),
        "total_return_percent": round(total_return_percent, 2),
        "recommendation": _label_from_score(base["portfolio_score"]),
        "portfolio_score": base["portfolio_score"],
        "confidence": base["portfolio_confidence"],
        "diversification_score": base["diversification_score"],
        "concentration_score": base["concentration_score"],
        "risk_score": base["risk_score"],
        "risk_level": base["risk_level"],
        "sector_exposure": sector_concentration,
        "holdings": holdings,
        "recommendations": base["recommendations"],
        "top_risks": _dedupe_list(top_risks, limit=6),
        "top_opportunities": _dedupe_list(top_opportunities, limit=6),
        "ai_summary": ai_summary,
        "best_holding": (
            {
                "symbol": best_holding["symbol"],
                "return_percent": round(_safe_float(best_holding.get("return_percent"), 0.0), 2),
                "gain_loss": round(_safe_float(best_holding.get("gain_loss"), 0.0), 2),
            }
            if best_holding
            else None
        ),
        "worst_holding": (
            {
                "symbol": worst_holding["symbol"],
                "return_percent": round(_safe_float(worst_holding.get("return_percent"), 0.0), 2),
                "gain_loss": round(_safe_float(worst_holding.get("gain_loss"), 0.0), 2),
            }
            if worst_holding
            else None
        ),
        "insight_score": insight_score,
    }