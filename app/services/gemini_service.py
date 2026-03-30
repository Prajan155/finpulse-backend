from typing import Iterable, List
from google import genai
from app.core.config import settings

client = genai.Client(api_key=settings.gemini_api_key)


def generate_firepulse_response(prompt: str) -> str:
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )
    return response.text or ""


def embed_texts(texts: Iterable[str]) -> List[List[float]]:
    items = [str(t).strip() for t in texts if str(t).strip()]
    if not items:
        return []

    result = client.models.embed_content(
        model="gemini-embedding-001",
        contents=items,
    )

    embeddings = getattr(result, "embeddings", None)
    if embeddings is None and isinstance(result, dict):
        embeddings = result.get("embeddings", [])

    vectors: List[List[float]] = []

    for emb in embeddings or []:
        values = getattr(emb, "values", None)

        if values is None and isinstance(emb, dict):
            values = emb.get("values")

        if values is None:
            try:
                values = emb["values"]
            except Exception:
                values = None

        if values is not None:
            vectors.append([float(v) for v in values])

    return vectors


# 🔥 ADD THIS (CRITICAL)
def embed_one(text: str):
    vectors = embed_texts([text])
    if not vectors:
        return None
    return vectors[0]