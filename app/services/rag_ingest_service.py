from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Dict, List

import chromadb

from app.services.gemini_service import embed_texts

CHROMA_DIR = Path(os.getenv("CHROMA_DIR", "/tmp/chroma"))
KNOWLEDGE_DIR = Path(os.getenv("KNOWLEDGE_DIR", "/tmp/knowledge"))
COLLECTION_NAME = "firepulse_knowledge"


def _get_client():
    CHROMA_DIR.mkdir(parents=True, exist_ok=True)
    return chromadb.PersistentClient(path=str(CHROMA_DIR))


def _get_collection():
    client = _get_client()
    return client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},
    )


def _chunk_text(text: str, chunk_size: int = 900, overlap: int = 150) -> List[str]:
    text = " ".join(text.split()).strip()
    if not text:
        return []

    chunks = []
    step = max(1, chunk_size - overlap)

    for i in range(0, len(text), step):
        chunk = text[i:i + chunk_size].strip()
        if chunk:
            chunks.append(chunk)

    return chunks


def _parse_metadata_from_path(path: Path) -> Dict[str, str | None]:
    stem = path.stem
    symbol = None
    topic = "general"

    if "__" in stem:
        left, right = stem.split("__", 1)
        symbol = left.strip().upper()
        topic = right.strip().lower()

    return {
        "symbol": symbol or "",
        "topic": topic,
        "source": path.suffix.replace(".", "") or "txt",
        "path": str(path),
    }


def _read_file(path: Path) -> str:
    try:
        if path.suffix.lower() in [".txt", ".md"]:
            return path.read_text(encoding="utf-8", errors="ignore")

        if path.suffix.lower() == ".json":
            data = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
            return json.dumps(data, indent=2)
    except Exception as exc:
        print("File read error:", path, exc)

    return ""


def _make_chunk_id(path: Path, idx: int, chunk: str) -> str:
    raw = f"{path}::{idx}::{chunk}"
    return hashlib.md5(raw.encode()).hexdigest()


def ingest_knowledge_base():
    CHROMA_DIR.mkdir(parents=True, exist_ok=True)
    KNOWLEDGE_DIR.mkdir(parents=True, exist_ok=True)

    print("KNOWLEDGE_DIR:", KNOWLEDGE_DIR)
    print("CHROMA_DIR:", CHROMA_DIR)

    collection = _get_collection()

    files = []
    for ext in ("*.txt", "*.md", "*.json"):
        files.extend(KNOWLEDGE_DIR.rglob(ext))

    print("FILES FOUND:", files)

    if not files:
        return {"ok": False, "message": "No knowledge files found"}

    all_docs: List[str] = []
    all_ids: List[str] = []
    all_metas: List[Dict] = []

    for path in files:
        text = _read_file(path)
        if not text.strip():
            continue

        meta = _parse_metadata_from_path(path)
        chunks = _chunk_text(text)

        for i, chunk in enumerate(chunks):
            chunk_id = _make_chunk_id(path, i, chunk)
            all_docs.append(chunk)
            all_ids.append(chunk_id)
            all_metas.append({**meta, "chunk_index": i})

    if not all_docs:
        return {"ok": False, "message": "No valid content found"}

    existing_ids = set()
    try:
        existing = collection.get(include=[])
        existing_ids = set(existing.get("ids", []) or [])
    except Exception as exc:
        print("Warning: could not read existing IDs from Chroma:", exc)

    new_docs: List[str] = []
    new_ids: List[str] = []
    new_metas: List[Dict] = []

    for doc, doc_id, meta in zip(all_docs, all_ids, all_metas):
        if doc_id in existing_ids:
            continue
        new_docs.append(doc)
        new_ids.append(doc_id)
        new_metas.append(meta)

    if not new_docs:
        return {
            "ok": True,
            "chunks_indexed": 0,
            "message": "No new chunks to index",
        }

    print("Embedding new chunks:", len(new_docs))
    vectors = embed_texts(new_docs)
    vectors = [list(map(float, v)) for v in vectors]

    collection.add(
        ids=new_ids,
        documents=new_docs,
        metadatas=new_metas,
        embeddings=vectors,
    )

    return {
        "ok": True,
        "chunks_indexed": len(new_docs),
        "message": "Indexed only new chunks",
    }