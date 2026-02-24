# Multi-Domain NL→SQL Copilot (RAG + Ollama + Postgres + Streamlit)

An end-to-end **Natural Language → SQL** system that works across **multiple database domains** using **schema-aware retrieval (RAG)**, a **local LLM (Ollama)**, strict **SQL guardrails**, a **self-correction loop**, and a **Streamlit UI**.

---

## What this project does

1. Select a domain (Chinook / DVDRental / Northwind)
2. Ask a question in plain English
3. Retrieve relevant schema context via **Chroma** (RAG)
4. Generate SQL with **Ollama** (local LLM)
5. Enforce safety with **guardrails** (SELECT/WITH only + LIMIT)
6. Self-correct SQL on execution errors / suspicious outputs (retry loop)
7. Execute SQL on Postgres and show results + CSV download in Streamlit

---

## Key Features

- **Multi-domain** querying across 3 Postgres databases:
  - `chinook` (music store)
  - `dvdrental` (movie rental)
  - `northwind` (orders/sales)
- **Schema RAG**:
  - Extract schema → chunk tables → embed → retrieve Top-K relevant schema chunks
- **Relationship-aware retrieval**:
  - Always injects a **Foreign Key join map** (RELATIONSHIPS chunk) into context
- **Domain glossary prompting**:
  - Northwind revenue/value formula includes discount
  - DVDRental revenue uses payments
  - Chinook revenue uses invoice totals
- **Few-shot examples** per domain for higher SQL accuracy and correct join paths
- **Guardrails**:
  - single statement only
  - **SELECT/WITH only** (blocks INSERT/UPDATE/DELETE/DROP/ALTER)
  - **LIMIT enforced**
- **Streamlit UI**:
  - Retrieve → Generate SQL → Run
  - Transparency panels (retrieved chunks + SQL + results)
  - CSV download
- **Execution-based evaluation**:
  - Compare predicted SQL vs gold SQL by executing both and comparing results

---

## Tech Stack

- Python
- PostgreSQL (Docker)
- ChromaDB (vector store)
- sentence-transformers (schema embeddings)
- Ollama (local LLM inference)
- Streamlit (UI)
- Pandas (evaluation + results)

---

## Architecture (High Level)

**Question**  
→ **Retriever (Chroma)**: Top-K schema chunks + **Relationships (FK join map)**  
→ **Prompt Builder**: rules + glossary + dynamic guidance + few-shot + schema context  
→ **LLM (Ollama)** generates SQL  
→ **Guardrails** validate SQL (SELECT-only + LIMIT)  
→ **Self-correction loop** retries on SQL errors / suspicious outputs  
→ **Postgres execution**  
→ **Results + CSV download**

## Results (Execution-Based Evaluation)

Evaluation is done by generating SQL for each question, enforcing guardrails (SELECT/WITH-only + LIMIT),
executing **both predicted SQL and gold SQL** on PostgreSQL, and comparing the returned result tables.

**Benchmark:** 59 gold queries across 3 domains (Chinook, DVDRental, Northwind)  
**Model:** `qwen2.5-coder:7b` (Ollama)  
**Retrieval:** Schema RAG (Chroma) + relationship join-map chunk, Top-K = 15  
**Safety:** Guardrails enabled (SELECT-only + LIMIT)

### Summary Metrics
- **Total cases:** 59  
- **Guardrail pass (pred):** 59 / 59 (**100%**)  
- **Guardrail pass (gold):** 59 / 59 (**100%**)  
- **Pred SQL executed:** 57 / 59 (**96.61%**)  
- **Gold SQL executed:** 57 / 59 (**96.61%**)  
- **Executed BOTH pred + gold:** 57 / 59 (**96.61%**)  
- **Execution accuracy (overall):** 50 / 59 (**84.75%**)  
- **Accuracy on executed-both:** 50 / 57 (**87.72%**)  

Detailed per-question results are saved to: `eval/report.csv`.






