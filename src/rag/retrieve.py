# src/rag/retrieve.py
from __future__ import annotations
from pathlib import Path
from typing import List, Dict, Any
import chromadb
from chromadb.utils import embedding_functions


def _add_relationship_chunk(collection, domain: str, out: List[Dict[str, Any]]) -> None:
    """
    Always add the relationship/join-map chunk (if present) to the top of context.
    """
    rel_id = f"{domain}::relationships"
    try:
        got = collection.get(ids=[rel_id])
        docs = got.get("documents", [])
        metas = got.get("metadatas", [])
        if docs:
            rel_doc = docs[0]
            rel_meta = metas[0] if metas else {"type": "relationships", "domain": domain}

            # avoid duplicates if already retrieved by similarity
            for item in out:
                if item.get("meta", {}).get("type") == "relationships":
                    return

            out.insert(0, {"text": rel_doc, "meta": rel_meta, "distance": 0.0})
    except Exception:
        # If not found, ignore
        pass


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
        raise FileNotFoundError(f"Index not found: {persist_dir}. Build it with scripts/build_index.py")

    emb_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name=embedding_model
    )

    client = chromadb.PersistentClient(path=str(persist_dir))
    collection = client.get_collection(name=collection_name, embedding_function=emb_fn)

    res = collection.query(query_texts=[question], n_results=k)
    docs = res.get("documents", [[]])[0]
    metas = res.get("metadatas", [[]])[0]
    dists = res.get("distances", [[]])[0]

    out: List[Dict[str, Any]] = []
    for doc, meta, dist in zip(docs, metas, dists):
        out.append({"text": doc, "meta": meta, "distance": dist})

    # ✅ Ensure join-map is included
    _add_relationship_chunk(collection, domain, out)

    return out
