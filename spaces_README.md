---
title: NL2SQL Copilot
emoji: 🧠
colorFrom: indigo
colorTo: blue
sdk: docker
app_port: 7860
pinned: false
license: mit
---

# 🧠 Multi-Domain NL→SQL Copilot

A production-quality **Natural Language → SQL** system that converts plain English questions into executable SQL across three databases — using **schema-aware RAG**, **OpenAI GPT-4o-mini**, and strict **SQL guardrails**.

## Live Demo

Enter your OpenAI API key in the sidebar, pick a database (Chinook or Northwind), ask a question, and click **Run SQL**.

Example questions:
- *"Show me the top 5 customers by total spend"*
- *"Which genres have more than 100 tracks?"*
- *"List all products with less than 10 units in stock"*

## How It Works

1. **Schema RAG** — your question is embedded and matched against pre-indexed schema chunks (ChromaDB + sentence-transformers)
2. **Prompt construction** — top-K schema chunks + domain glossary → structured prompt
3. **LLM generation** — GPT-4o-mini generates SQL (SELECT-only, guardrails enforced)
4. **Safe execution** — SQL is validated (no DDL/DML), LIMIT is injected, then run against a bundled SQLite database
5. **Results** — table view + CSV download

## Benchmark Results

| Model | Accuracy (59 gold queries) | Avg Latency | Cost |
|---|---|---|---|
| gpt-4o-mini (OpenAI) | **100%** | 1.4 s | ~$0.15/59 queries |
| qwen2.5-coder:7b (local Ollama) | **98.3%** | 4.8 s | $0.00 |
| Groq llama-3.3-70b (free cloud) | 52.5% | 7.3 s | $0.00 |
| Groq Qwen3-32B reasoning (free) | 49.2% | 34.5 s | $0.00 |

> **Key insight:** A 7B SQL-specialized local model (qwen2.5-coder:7b) nearly matches GPT-4o-mini at zero cost, while outperforming a 32B reasoning cloud model by **49 percentage points** — task-specific fine-tuning beats raw model size.

## Run Locally (full Postgres + Ollama stack)

```bash
git clone https://github.com/YOUR_USERNAME/nl2sql2
cd nl2sql2
cp .env.example .env   # add your OPENAI_API_KEY
docker-compose up --build
# App → http://localhost:8501
```

## Tech Stack

- **RAG**: ChromaDB + sentence-transformers (`all-MiniLM-L6-v2`)
- **LLM**: OpenAI GPT-4o-mini (Spaces) / Ollama qwen2.5-coder:7b (local)
- **DB**: SQLite (Spaces) / PostgreSQL (local Docker)
- **Guardrails**: sqlglot AST parsing — SELECT/WITH only, LIMIT enforcement
- **UI**: Streamlit
- **Tests**: 64 pytest unit tests
