from __future__ import annotations

from typing import Dict, List

import chromadb

from app.services.gemini_service import embed_one
from app.services.rag_ingest_service import CHROMA_DIR, COLLECTION_NAME


def _get_client():
    return chromadb.PersistentClient(path=str(CHROMA_DIR))


def _get_collection():
    client = _get_client()
    return client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},
    )


def _make_label_from_path(path: str) -> str:
    name = path.split("\\")[-1].split("/")[-1]
    name = name.replace(".txt", "").replace(".md", "").replace(".json", "")
    return name.replace("__", " - ")


def retrieve_context(question: str, symbol: str | None = None, top_k: int = 5) -> List[Dict]:
    collection = _get_collection()
    query_vector = embed_one(question)

    if not query_vector:
        return []

    results: List[Dict] = []
    seen_ids = set()

    def _run_query(where: Dict | None, n_results: int):
        return collection.query(
            query_embeddings=[query_vector],
            n_results=n_results,
            where=where,
            include=["documents", "metadatas", "distances"],
        )

    if symbol:
        symbol_res = _run_query(where={"symbol": symbol.upper().strip()}, n_results=top_k)
        ids = symbol_res.get("ids", [[]])[0]
        docs = symbol_res.get("documents", [[]])[0]
        metas = symbol_res.get("metadatas", [[]])[0]
        dists = symbol_res.get("distances", [[]])[0]

        for i, doc_id in enumerate(ids):
            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)

            path = metas[i].get("path", "")
            results.append(
                {
                    "id": doc_id,
                    "text": docs[i],
                    "score": float(dists[i]) if i < len(dists) else None,
                    "source": path,
                    "label": _make_label_from_path(path),
                    "symbol": metas[i].get("symbol") or None,
                    "topic": metas[i].get("topic") or None,
                }
            )

    remaining = max(0, top_k - len(results))
    if remaining > 0:
        general_res = _run_query(where={"symbol": ""}, n_results=max(remaining, 2))
        ids = general_res.get("ids", [[]])[0]
        docs = general_res.get("documents", [[]])[0]
        metas = general_res.get("metadatas", [[]])[0]
        dists = general_res.get("distances", [[]])[0]

        for i, doc_id in enumerate(ids):
            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)

            path = metas[i].get("path", "")
            results.append(
                {
                    "id": doc_id,
                    "text": docs[i],
                    "score": float(dists[i]) if i < len(dists) else None,
                    "source": path,
                    "label": _make_label_from_path(path),
                    "symbol": metas[i].get("symbol") or None,
                    "topic": metas[i].get("topic") or None,
                }
            )
            if len(results) >= top_k:
                break

    results.sort(
        key=lambda x: (
            0 if x.get("symbol") else 1,
            x.get("score") if x.get("score") is not None else 9999,
        )
    )

    unique_texts = set()
    filtered: List[Dict] = []

    for item in results:
        key = (item.get("text") or "")[:100].strip().lower()
        if not key or key in unique_texts:
            continue
        unique_texts.add(key)
        filtered.append(item)

    return filtered[:top_k]