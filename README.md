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
**Retrieval:** Schema RAG (Chroma) + relationship join-map chunk, Top-K = 15  
**Safety:** Guardrails enabled (SELECT-only + LIMIT)

### Model Comparison (59 gold queries · 3 domains)

| Model | Accuracy | Avg Latency | Cost |
|---|---|---|---|
| `gpt-4o-mini` (OpenAI) | **59 / 59 (100%)** | 1.4 s | $0.15 total |
| `qwen2.5-coder:7b` (Ollama, local) | **58 / 59 (98.3%)** | 4.8 s | **$0.00** |
| `llama-3.3-70b` (Groq, free cloud) | 31 / 59 (52.5%) | 7.3 s | **$0.00** |
| `Qwen3-32B` reasoning (Groq, free) | 29 / 59 (49.2%) | 34.5 s | **$0.00** |

> **Key finding:** A 7B SQL-specialized local model matches GPT-4o-mini within 1.7% at zero cost — and outperforms a 32B reasoning cloud model by **49 percentage points**. Task-specific fine-tuning beats raw model size.

### Key Takeaway

`qwen2.5-coder:7b` running locally on CPU achieves **98.3% execution accuracy** — within 1 query of GPT-4o-mini — at $0 API cost. Both large general-purpose cloud models (llama-3.3-70b and Qwen3-32B) score ~50% despite being 4-5× larger, confirming that **model specialization matters more than scale** for domain-specific SQL generation.

🔗 **Live demo:** https://huggingface.co/spaces/DEKU02/nl2sql

Detailed per-query results: `eval/compare_report.csv` | Summary: `eval/compare_summary.csv`






