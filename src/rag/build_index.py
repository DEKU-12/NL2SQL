# src/rag/build_index.py
from __future__ import annotations
from pathlib import Path
import chromadb
from chromadb.utils import embedding_functions

from .chunk_schema import chunk_schema
from .chunk_relationships import relationship_chunk  # ✅ NEW


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

    # --- Table chunks ---
    chunks = chunk_schema(schema_path)
    ids = [cid for cid, _, _ in chunks]
    docs = [txt for _, txt, _ in chunks]
    metas = [m for _, _, m in chunks]

    # --- ✅ Relationship chunk (join map) ---
    rel = relationship_chunk(str(schema_path))
    ids.append(rel["id"])
    docs.append(rel["text"])
    metas.append(rel["meta"])

    # Avoid duplicates: delete if already present
    try:
        existing = collection.get(ids=ids)
        if existing and existing.get("ids"):
            collection.delete(ids=ids)
    except Exception:
        pass

    # Safety check (avoid empty add)
    if not ids or not docs or not metas:
        raise ValueError(f"No chunks to add for domain={domain}. Check schema_path={schema_path}")

    collection.add(ids=ids, documents=docs, metadatas=metas)
