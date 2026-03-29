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
    items = [t.strip() for t in texts if t and t.strip()]
    if not items:
        return []

    result = client.models.embed_content(
        model="gemini-embedding-001",
        contents=items,
    )

    embeddings = getattr(result, "embeddings", None) or []
    vectors: List[List[float]] = []

    for emb in embeddings:
        values = getattr(emb, "values", None)
        if values is None and isinstance(emb, dict):
            values = emb.get("values")
        vectors.append(list(values or []))

    return vectors


def embed_one(text: str) -> List[float]:
    vectors = embed_texts([text])
    return vectors[0] if vectors else []