from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Dict, List

import chromadb

from app.services.gemini_service import embed_texts

BASE_DIR = Path(__file__).resolve().parents[2]
KNOWLEDGE_DIR = BASE_DIR / "knowledge_base"
CHROMA_DIR = BASE_DIR / ".chroma_firepulse"
COLLECTION_NAME = "firepulse_knowledge"


def _get_client():
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

    chunks: List[str] = []
    step = max(1, chunk_size - overlap)
    start = 0
    n = len(text)

    while start < n:
        end = min(start + chunk_size, n)
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= n:
            break
        start += step

    return chunks


def _parse_metadata_from_path(path: Path) -> Dict[str, str | None]:
    stem = path.stem

    symbol = None
    topic = "general"

    if "__" in stem:
        left, right = stem.split("__", 1)
        symbol = left.strip().upper()
        topic = right.strip().lower()

    source = path.suffix.lower().replace(".", "") or "txt"

    return {
        "symbol": symbol,
        "topic": topic,
        "source": source,
        "path": str(path.relative_to(BASE_DIR)),
    }


def _read_file(path: Path) -> str:
    suffix = path.suffix.lower()

    try:
        if suffix in {".txt", ".md"}:
            return path.read_text(encoding="utf-8", errors="ignore")

        if suffix == ".json":
            data = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
            return json.dumps(data, indent=2, ensure_ascii=False)

    except Exception as exc:
        print(f"❌ Failed to read file {path}: {exc}")
        return ""

    return ""


def _make_chunk_id(path: Path, idx: int, chunk: str) -> str:
    raw = f"{path.as_posix()}::{idx}::{chunk}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def ingest_knowledge_base() -> Dict[str, int | List[str]]:
    collection = _get_collection()

    if not KNOWLEDGE_DIR.exists():
        return {
            "files": 0,
            "total_chunks_found": 0,
            "chunks_indexed": 0,
            "chunks_skipped_existing": 0,
            "errors": ["knowledge_base folder not found"],
        }

    files: List[Path] = []
    for ext in ("*.txt", "*.md", "*.json"):
        files.extend(KNOWLEDGE_DIR.rglob(ext))

    all_ids: List[str] = []
    all_docs: List[str] = []
    all_metas: List[Dict] = []
    errors: List[str] = []

    for path in files:
        print(f"📄 Processing file: {path}")

        text = _read_file(path)
        print(f"   Text length: {len(text)}")
        print(f"   Preview: {repr(text[:200])}")

        if not text.strip():
            print("   Skipped: empty or unreadable")
            continue

        meta = _parse_metadata_from_path(path)
        chunks = _chunk_text(text)
        print(f"   Chunks created: {len(chunks)}")

        if not chunks:
            continue

        for idx, chunk in enumerate(chunks):
            chunk_id = _make_chunk_id(path, idx, chunk)
            chunk_meta = {
                "symbol": meta["symbol"] or "",
                "topic": meta["topic"] or "general",
                "source": meta["source"] or "txt",
                "path": meta["path"],
                "chunk_index": idx,
            }
            all_ids.append(chunk_id)
            all_docs.append(chunk)
            all_metas.append(chunk_meta)

    total_chunks_found = len(all_docs)

    if not all_docs:
        return {
            "files": len(files),
            "total_chunks_found": 0,
            "chunks_indexed": 0,
            "chunks_skipped_existing": 0,
            "errors": errors,
        }

    print(f"🧠 Embedding {len(all_docs)} chunks...")
    vectors = embed_texts(all_docs)

    if not vectors:
        return {
            "files": len(files),
            "total_chunks_found": total_chunks_found,
            "chunks_indexed": 0,
            "chunks_skipped_existing": 0,
            "errors": ["embedding generation returned no vectors"],
        }

    if len(vectors) != len(all_docs):
        return {
            "files": len(files),
            "total_chunks_found": total_chunks_found,
            "chunks_indexed": 0,
            "chunks_skipped_existing": 0,
            "errors": [
                f"embedding count mismatch: docs={len(all_docs)}, vectors={len(vectors)}"
            ],
        }

    existing_result = collection.get()
    existing_ids = set(existing_result.get("ids", [])) if existing_result else set()

    keep_ids: List[str] = []
    keep_docs: List[str] = []
    keep_metas: List[Dict] = []
    keep_vectors: List[List[float]] = []

    skipped_existing = 0

    for i, doc_id in enumerate(all_ids):
        if doc_id in existing_ids:
            skipped_existing += 1
            continue

        keep_ids.append(doc_id)
        keep_docs.append(all_docs[i])
        keep_metas.append(all_metas[i])
        keep_vectors.append(vectors[i])

    if keep_docs:
        collection.add(
            ids=keep_ids,
            documents=keep_docs,
            metadatas=keep_metas,
            embeddings=keep_vectors,
        )

    return {
        "files": len(files),
        "total_chunks_found": total_chunks_found,
        "chunks_indexed": len(keep_docs),
        "chunks_skipped_existing": skipped_existing,
        "errors": errors,
    }