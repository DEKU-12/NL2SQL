# src/rag/retrieve.py
from __future__ import annotations
import re
from collections import deque
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


def _expand_join_paths(collection, out: List[Dict[str, Any]]) -> None:
    """
    Add bridge tables: for every pair of retrieved tables, add the tables on the
    shortest foreign-key path between them (e.g. reviews <-> items needs orders).
    Similarity search alone misses these because bridges don't look like the question.
    """
    rel = next((c["text"] for c in out if c.get("meta", {}).get("type") == "relationships"), "")
    graph: Dict[str, set] = {}
    for a, b in re.findall(r"^- (\w+)\.\w+ -> (\w+)\.\w+$", rel, flags=re.M):
        graph.setdefault(a, set()).add(b)
        graph.setdefault(b, set()).add(a)

    have = [c["meta"]["table"] for c in out if c.get("meta", {}).get("table")]
    needed: List[str] = []
    for i, src in enumerate(have):
        for dst in have[i + 1:]:
            # BFS shortest path src -> dst
            prev = {src: None}
            queue = deque([src])
            while queue and dst not in prev:
                node = queue.popleft()
                for nxt in graph.get(node, ()):
                    if nxt not in prev:
                        prev[nxt] = node
                        queue.append(nxt)
            node = prev.get(dst)
            while node and node != src:
                if node not in have and node not in needed:
                    needed.append(node)
                node = prev[node]

    if needed:
        got = collection.get(ids=[f"{t}__schema" for t in needed])
        for doc, meta in zip(got.get("documents", []), got.get("metadatas", [])):
            out.append({"text": doc, "meta": meta, "distance": None})


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
    _expand_join_paths(collection, out)

    return out
