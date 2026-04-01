from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import requests
from bs4 import BeautifulSoup

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
KNOWLEDGE_DIR = Path(os.getenv("KNOWLEDGE_DIR", "/tmp/knowledge"))
NEWS_DIR = KNOWLEDGE_DIR / "news"

DEFAULT_NEWS_SYMBOLS = [
    s.strip().upper()
    for s in os.getenv(
        "FIREPULSE_NEWS_SYMBOLS",
        "TCS.NS,RELIANCE.NS,INFY.NS,HDFCBANK.NS,SBIN.NS,AAPL,MSFT,NVDA,GOOGL,TSLA",
    ).split(",")
    if s.strip()
]

BULLISH_KEYWORDS = [
    "beat",
    "beats",
    "strong",
    "growth",
    "upgrade",
    "upgrades",
    "surge",
    "record",
    "profit",
    "profits",
    "gain",
    "gains",
    "bullish",
    "buyback",
    "contract",
    "orders",
    "order win",
    "partnership",
    "launch",
    "expansion",
    "outperform",
    "raises",
    "raised",
    "ai",
]

BEARISH_KEYWORDS = [
    "miss",
    "misses",
    "weak",
    "decline",
    "falls",
    "drop",
    "drops",
    "cut",
    "cuts",
    "downgrade",
    "downgrades",
    "lawsuit",
    "probe",
    "investigation",
    "delay",
    "delays",
    "loss",
    "losses",
    "bearish",
    "warning",
    "selloff",
    "sell-off",
    "recall",
    "fraud",
    "penalty",
    "slump",
]

HIGH_IMPACT_KEYWORDS = [
    "earnings",
    "results",
    "guidance",
    "merger",
    "acquisition",
    "lawsuit",
    "probe",
    "investigation",
    "buyback",
    "contract",
    "order",
    "policy",
    "regulation",
    "tariff",
    "ban",
    "approval",
    "launch",
    "forecast",
    "quarter",
    "revenue",
    "profit",
]


def _normalize_finnhub_symbol(symbol: str) -> Tuple[str, str]:
    raw = str(symbol or "").strip().upper()
    if not raw:
        return "", ""

    if raw.endswith(".NS") or raw.endswith(".BO"):
        return raw.split(".")[0], raw

    return raw, raw


def _safe_filename_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper().replace("/", "_").replace("\\", "_")


def _score_sentiment_and_impact(headline: str, summary: str) -> Dict[str, str]:
    text = f"{headline} {summary}".lower()

    bullish_hits = sum(1 for kw in BULLISH_KEYWORDS if kw in text)
    bearish_hits = sum(1 for kw in BEARISH_KEYWORDS if kw in text)
    high_impact_hits = sum(1 for kw in HIGH_IMPACT_KEYWORDS if kw in text)

    if bullish_hits > bearish_hits:
        sentiment = "Bullish"
        sentiment_reason = "Positive catalysts or favorable language detected."
    elif bearish_hits > bullish_hits:
        sentiment = "Bearish"
        sentiment_reason = "Negative catalysts or risk language detected."
    else:
        sentiment = "Neutral"
        sentiment_reason = "Mixed or limited directional signals detected."

    text_len = len(text)
    if high_impact_hits >= 2 or text_len > 600:
        impact = "High"
        impact_reason = "Major catalyst keywords or dense market-moving context detected."
    elif high_impact_hits == 1 or text_len > 250:
        impact = "Medium"
        impact_reason = "Some potentially relevant catalyst signals detected."
    else:
        impact = "Low"
        impact_reason = "Limited evidence of a major near-term catalyst."

    return {
        "sentiment": sentiment,
        "impact": impact,
        "sentiment_reason": sentiment_reason,
        "impact_reason": impact_reason,
    }


def fetch_google_news(symbol: str, limit: int = 5) -> List[Dict]:
    try:
        query = str(symbol or "").strip().upper()
        query = query.replace(".NS", "").replace(".BO", "")
        if not query:
            return []

        url = (
            "https://news.google.com/rss/search"
            f"?q={query}+stock&hl=en-IN&gl=IN&ceid=IN:en"
        )

        response = requests.get(
            url,
            timeout=8,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            },
        )
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")
        rss_items = soup.find_all("item")

        news: List[Dict] = []
        for item in rss_items[: max(1, int(limit or 5))]:
            headline = item.title.text.strip() if item.title and item.title.text else ""
            summary = (
                item.description.text.strip()
                if item.description and item.description.text
                else ""
            )
            link = item.link.text.strip() if item.link and item.link.text else ""

            scores = _score_sentiment_and_impact(headline, summary)

            news.append(
                {
                    "headline": headline,
                    "summary": summary,
                    "source": "Google News",
                    "url": link,
                    "datetime": None,
                    "sentiment": scores["sentiment"],
                    "impact": scores["impact"],
                    "sentiment_reason": scores["sentiment_reason"],
                    "impact_reason": scores["impact_reason"],
                }
            )

        return news
    except Exception as exc:
        print(f"Google News fetch failed for {symbol}: {exc}")
        return []


def fetch_company_news(symbol: str, days_back: int = 14, limit: int = 5) -> List[Dict]:
    finnhub_symbol, original_symbol = _normalize_finnhub_symbol(symbol)
    if not original_symbol:
        return []

    if FINNHUB_API_KEY:
        try:
            today = date.today()
            start_date = today - timedelta(days=max(1, int(days_back or 14)))

            url = "https://finnhub.io/api/v1/company-news"
            params = {
                "symbol": finnhub_symbol,
                "from": start_date.isoformat(),
                "to": today.isoformat(),
                "token": FINNHUB_API_KEY,
            }

            response = requests.get(url, params=params, timeout=8)
            response.raise_for_status()

            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                items: List[Dict] = []
                for item in data[: max(1, int(limit or 5))]:
                    headline = item.get("headline") or ""
                    summary = item.get("summary") or ""
                    scores = _score_sentiment_and_impact(headline, summary)

                    items.append(
                        {
                            "headline": headline,
                            "summary": summary,
                            "source": item.get("source"),
                            "url": item.get("url"),
                            "datetime": item.get("datetime"),
                            "sentiment": scores["sentiment"],
                            "impact": scores["impact"],
                            "sentiment_reason": scores["sentiment_reason"],
                            "impact_reason": scores["impact_reason"],
                        }
                    )
                return items
        except Exception as exc:
            print(f"Finnhub failed for {symbol}: {exc}")

    print(f"⚡ Using Google News fallback for {original_symbol}")
    return fetch_google_news(original_symbol, limit=limit)


def summarize_news(news_items: List[Dict]) -> str:
    if not news_items:
        return "No recent news available."

    lines: List[str] = []
    for idx, item in enumerate(news_items[:5], start=1):
        headline = str(item.get("headline") or "").strip()
        summary = str(item.get("summary") or "").strip()
        source = str(item.get("source") or "").strip()
        sentiment = str(item.get("sentiment") or "Neutral").strip()
        impact = str(item.get("impact") or "Low").strip()

        if not headline:
            continue

        line = f"{idx}. {headline} [{source or 'Unknown'} | Sentiment: {sentiment} | Impact: {impact}]"
        if summary:
            line += f" — {summary}"
        lines.append(line)

    return "\n".join(lines) if lines else "No recent news available."


def _format_news_markdown(symbol: str, news_items: List[Dict]) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    lines = [
        f"# {symbol} latest news",
        "",
        f"Generated at: {now_utc}",
        "",
    ]

    if not news_items:
        lines.append("No recent news available.")
        lines.append("")
        return "\n".join(lines)

    for idx, item in enumerate(news_items, start=1):
        headline = str(item.get("headline") or "").strip() or "Untitled"
        summary = str(item.get("summary") or "").strip() or "No summary available."
        source = str(item.get("source") or "").strip() or "Unknown source"
        url = str(item.get("url") or "").strip()
        timestamp = item.get("datetime")
        sentiment = str(item.get("sentiment") or "Neutral").strip()
        impact = str(item.get("impact") or "Low").strip()
        sentiment_reason = str(item.get("sentiment_reason") or "").strip()
        impact_reason = str(item.get("impact_reason") or "").strip()

        readable_time = ""
        try:
            if timestamp:
                readable_time = datetime.fromtimestamp(
                    int(timestamp), tz=timezone.utc
                ).isoformat()
        except Exception:
            readable_time = ""

        lines.append(f"## {idx}. {headline}")
        lines.append(f"- Source: {source}")
        lines.append(f"- Sentiment: {sentiment}")
        lines.append(f"- Impact: {impact}")
        if sentiment_reason:
            lines.append(f"- Sentiment reason: {sentiment_reason}")
        if impact_reason:
            lines.append(f"- Impact reason: {impact_reason}")
        if readable_time:
            lines.append(f"- Published: {readable_time}")
        if url:
            lines.append(f"- URL: {url}")
        lines.append("")
        lines.append(summary)
        lines.append("")

    return "\n".join(lines)


def write_news_snapshot(original_symbol: str, news_items: List[Dict]) -> str:
    NEWS_DIR.mkdir(parents=True, exist_ok=True)

    safe_symbol = _safe_filename_symbol(original_symbol)
    path = NEWS_DIR / f"{safe_symbol}__news.md"
    content = _format_news_markdown(symbol=original_symbol, news_items=news_items)
    path.write_text(content, encoding="utf-8")
    return str(path)


def refresh_news_knowledge(
    symbols: List[str] | None = None,
    days_back: int = 7,
    limit: int = 8,
) -> Dict:
    NEWS_DIR.mkdir(parents=True, exist_ok=True)

    target_symbols = [s for s in (symbols or DEFAULT_NEWS_SYMBOLS) if str(s).strip()]
    written_files: List[str] = []
    processed = 0
    empty_symbols: List[str] = []

    for symbol in target_symbols:
        processed += 1
        items = fetch_company_news(symbol, days_back=days_back, limit=limit)

        if not items:
            print(f"⚠️ No news for {symbol}")
            empty_symbols.append(symbol)

        try:
            written_files.append(write_news_snapshot(symbol, items))
        except Exception as exc:
            print(f"Failed writing news snapshot for {symbol}: {exc}")

    return {
        "ok": True,
        "symbols_processed": processed,
        "files_written": written_files,
        "empty_symbols": empty_symbols,
        "news_dir": str(NEWS_DIR),
    }