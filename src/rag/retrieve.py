# src/rag/retrieve.py
from __future__ import annotations
from pathlib import Path
from typing import List, Dict, Any
import chromadb
from chromadb.utils import embedding_functions


def retrieve_schema_chunks(
    domain: str,
    question: str,
    k: int = 6,
    persist_dir: str | Path = "data/chroma",
    collection_name: str = "schema_chunks",
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2",
) -> List[Dict[str, Any]]:
    persist_dir = Path(persist_dir) / domain
    if not persist_dir.exists():
        raise FileNotFoundError(f"Index not found: {persist_dir}. Build it with scripts/02_build_index.py")

    emb_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name=embedding_model
    )

    client = chromadb.PersistentClient(path=str(persist_dir))
    collection = client.get_collection(name=collection_name, embedding_function=emb_fn)

    res = collection.query(query_texts=[question], n_results=k)
    docs = res.get("documents", [[]])[0]
    metas = res.get("metadatas", [[]])[0]
    dists = res.get("distances", [[]])[0]

    out = []
    for doc, meta, dist in zip(docs, metas, dists):
        out.append({"text": doc, "meta": meta, "distance": dist})
    return out
