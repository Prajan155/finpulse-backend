from typing import Iterable, List, Optional
from google import genai
from app.core.config import settings
import time

client = genai.Client(api_key=settings.gemini_api_key)

EMBEDDING_MODEL = "gemini-embedding-001"

PREFERRED_GENERATION_MODELS = [
    "gemini-1.5-flash-latest",  # 🔥 cheaper first
    "gemini-2.0-flash",
    "gemini-2.5-flash",
]

_cached_generation_model: Optional[str] = None

# 🔥 NEW: RESPONSE CACHE
_RESPONSE_CACHE = {}
_CACHE_TTL = 300  # 5 min

# 🔥 NEW: COOLDOWN HANDLING
_LAST_FAIL_TIME = 0
_COOLDOWN = 60  # seconds


def _normalize_model_name(name: str) -> str:
    return str(name or "").replace("models/", "").strip()


def _pick_generation_model() -> str:
    global _cached_generation_model

    if _cached_generation_model:
        return _cached_generation_model

    try:
        models = client.models.list()
        available = [_normalize_model_name(m.name) for m in models]

        for candidate in PREFERRED_GENERATION_MODELS:
            if candidate in available:
                _cached_generation_model = candidate
                print(f"✅ Using Gemini model: {candidate}")
                return candidate
    except Exception as e:
        print("Model fetch failed:", e)

    return "gemini-1.5-flash-latest"


def generate_firepulse_response(prompt: str) -> str:
    global _LAST_FAIL_TIME

    now = time.time()

    # 🔥 COOLDOWN CHECK
    if now - _LAST_FAIL_TIME < _COOLDOWN:
        print("⚠️ Gemini cooldown active — skipping call")
        raise Exception("Cooldown active")

    # 🔥 CACHE CHECK
    cache_key = prompt[:500]
    if cache_key in _RESPONSE_CACHE:
        cached, ts = _RESPONSE_CACHE[cache_key]
        if now - ts < _CACHE_TTL:
            print("⚡ Using cached Gemini response")
            return cached

    model_name = _pick_generation_model()
    print(f"🔥 CALLING GEMINI... model={model_name}")

    try:
        response = client.models.generate_content(
            model=model_name,
            contents=prompt[:8000],  # 🔥 trim tokens
        )

        text = response.text or ""

        # 🔥 SAVE CACHE
        _RESPONSE_CACHE[cache_key] = (text, now)

        return text

    except Exception as e:
        print("❌ GEMINI FAILED:", e)

        # 🔥 ACTIVATE COOLDOWN
        _LAST_FAIL_TIME = time.time()

        raise e


def embed_texts(texts: Iterable[str]) -> List[List[float]]:
    items = [str(t).strip() for t in texts if str(t).strip()]
    if not items:
        return []

    result = client.models.embed_content(
        model=EMBEDDING_MODEL,
        contents=items,
    )

    embeddings = getattr(result, "embeddings", None)
    vectors = []

    for emb in embeddings or []:
        values = getattr(emb, "values", None)
        if values:
            vectors.append([float(v) for v in values])

    return vectors


def embed_one(text: str) -> Optional[List[float]]:
    vectors = embed_texts([text])
    return vectors[0] if vectors else None