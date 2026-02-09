# src/rag/build_index.py
from __future__ import annotations
from pathlib import Path
from typing import Optional
import chromadb
from chromadb.utils import embedding_functions

from .chunk_schema import chunk_schema


def build_domain_index(
    domain: str,
    schema_path: str | Path,
    persist_dir: str | Path = "data/chroma",
    collection_name: str = "schema_chunks",
    reset: bool = False,
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2",
) -> None:
    persist_dir = Path(persist_dir) / domain
    persist_dir.mkdir(parents=True, exist_ok=True)

    emb_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name=embedding_model
    )

    client = chromadb.PersistentClient(path=str(persist_dir))

    if reset:
        try:
            client.delete_collection(collection_name)
        except Exception:
            pass

    collection = client.get_or_create_collection(
        name=collection_name,
        embedding_function=emb_fn,
        metadata={"domain": domain},
    )

    chunks = chunk_schema(schema_path)
    ids = [cid for cid, _, _ in chunks]
    docs = [txt for _, txt, _ in chunks]
    metas = [m for _, _, m in chunks]

    # Avoid duplicates: upsert by deleting existing ids if present
    # (Chroma doesn't have a universal "upsert" in older versions)
    try:
        existing = collection.get(ids=ids)
        if existing and existing.get("ids"):
            collection.delete(ids=ids)
    except Exception:
        pass

    collection.add(ids=ids, documents=docs, metadatas=metas)
