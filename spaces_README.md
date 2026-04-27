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

A production-quality **Natural Language → SQL** system that converts plain English questions into executable SQL across three real-world databases — using **schema-aware RAG**, multiple LLM backends, and strict **SQL guardrails**.

## Live Demo

Pick a database, enter your API key in the sidebar, ask a question, and click **Run SQL**.

**Databases:**
- 🏙️ **NYC 311** — 300k+ government service requests (complaints, agencies, boroughs)
- 🛒 **Olist E-Commerce** — 100k+ Brazilian e-commerce orders (sellers, products, reviews)
- 🏥 **Synthea Healthcare** — 10k synthetic patients (encounters, conditions, medications)

**Example questions:**
- *"What are the top 5 most common complaint types across all boroughs?"*
- *"Which product category generates the most total revenue?"*
- *"How many patients have diabetes as a condition?"*

## How It Works

1. **Schema RAG** — your question is embedded and matched against pre-indexed schema chunks (ChromaDB + sentence-transformers)
2. **Prompt construction** — top-K schema chunks + domain glossary + few-shot examples → structured prompt
3. **LLM generation** — generates SQL (SELECT-only, guardrails enforced)
4. **Safe execution** — SQL is validated (no DDL/DML), LIMIT is injected, then run against a bundled SQLite database
5. **Results** — table view + CSV download

## Benchmark Results

| Model | Accuracy (59 gold queries) |
|---|---|
| OpenAI gpt-4o-mini | **96.6%** |
| Groq llama-3.3-70b-versatile (free) | **96.6%** |
| Groq llama-3.1-8b-instant (free) | 94.9% |
| Ollama llama3.2:3b (local) | 67.8% |

## Supported LLM Backends

| Backend | Key required | Cost |
|---|---|---|
| ⚡ Groq llama-3.3-70b | Free at console.groq.com | $0.00 |
| 🤖 OpenAI gpt-4o-mini | platform.openai.com | ~$0.03/59 queries |
| 🤗 HuggingFace Inference | huggingface.co/settings/tokens | Free tier |
| 🖥️ Ollama (local) | No key needed | $0.00 |

## Enabling the Olist Database

Olist data comes from Kaggle and cannot be downloaded during Docker build (secrets aren't available at that stage). To enable it:

1. Get a free Kaggle account at [kaggle.com](https://www.kaggle.com)
2. Go to **Settings → API → Create New Token** to get your credentials
3. In your Space settings, add two **Secrets**:
   - `KAGGLE_USERNAME` — your Kaggle username
   - `KAGGLE_KEY` — your API key
4. Restart the Space — Olist will build automatically on first boot (~2 min)

Without these secrets, the Space runs with NYC 311 and Synthea only (both auto-download).

## Tech Stack

- **RAG**: ChromaDB + sentence-transformers (`all-MiniLM-L6-v2`)
- **LLM**: Groq / OpenAI / HuggingFace / Ollama
- **DB**: SQLite (Spaces) / PostgreSQL (local Docker)
- **Guardrails**: sqlglot AST parsing — SELECT/WITH only, LIMIT enforcement
- **UI**: Streamlit
- **Tests**: 77 pytest unit tests
