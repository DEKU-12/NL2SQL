# 🧠 Multi-Domain NL→SQL Copilot

[![HF Spaces](https://img.shields.io/badge/🤗%20HuggingFace-Live%20Demo-blue)](https://huggingface.co/spaces/DEKU02/nl2sql)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-77%20passing-brightgreen)](#testing)

A production-quality **Natural Language → SQL** system that converts plain-English questions into executable SQL across three real-world databases — using **schema-aware RAG**, multiple LLM backends, and strict **SQL guardrails**.

**[▶ Try the live demo on HuggingFace Spaces](https://huggingface.co/spaces/DEKU02/nl2sql)**

---

## Benchmark Results

Evaluated on 59 hand-curated gold queries across all three domains:

| Model | Accuracy | Cost |
|---|---|---|
| OpenAI gpt-4o-mini | **96.6%** (57/59) | ~$0.03/run |
| Groq llama-3.3-70b-versatile | **96.6%** (57/59) | Free |
| Groq llama-3.1-8b-instant | 94.9% (56/59) | Free |
| Ollama llama3.2:3b (local) | 67.8% (40/59) | Free |

---

## Databases

| Domain | Tables | Scale | Source |
|---|---|---|---|
| 🏙️ **NYC 311** | 1 | 500k+ service requests | [NYC Open Data](https://data.cityofnewyork.us/resource/erm2-nwe9.csv) — auto-download |
| 🛒 **Olist E-Commerce** | 8 | 100k+ orders | [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — needs API key |
| 🏥 **Synthea Healthcare** | 6 | 10k synthetic patients | [Synthea](https://synthetichealth.github.io/synthea-sample-data/) — auto-download |

---

## How It Works

```
User question
     │
     ▼
[1] Schema RAG        — embed question → retrieve top-K schema chunks from ChromaDB
     │
     ▼
[2] Prompt Builder    — schema chunks + domain glossary + few-shot examples → prompt
     │
     ▼
[3] LLM Generation    — Groq / OpenAI / HuggingFace / Ollama generates SQL
     │
     ▼
[4] SQL Guardrails    — sqlglot AST: SELECT/WITH only, LIMIT injection, no DDL/DML
     │
     ▼
[5] Execution         — run against SQLite (Spaces) or PostgreSQL (local)
     │
     ▼
[6] Results           — table view + CSV download in Streamlit UI
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Vector store | ChromaDB + `sentence-transformers/all-MiniLM-L6-v2` |
| LLM backends | Groq · OpenAI · HuggingFace Inference API · Ollama |
| SQL guardrails | `sqlglot` AST parsing |
| Databases | SQLite (HF Spaces) · PostgreSQL (local Docker) |
| UI | Streamlit |
| Tests | pytest (77 tests) |

---

## Quick Start (Local)

### 1. Clone and install

```bash
git clone https://github.com/DEKU-12/NL2SQL.git
cd NL2SQL
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Build the databases

```bash
# NYC 311 and Synthea auto-download (~450 MB total, ~3 min)
python scripts/build_databases.py --only nyc311 synthea

# Olist requires a free Kaggle account:
# Get your token at https://www.kaggle.com/settings → API → Create New Token
export KAGGLE_API_TOKEN=KGAT_...
python scripts/build_databases.py --only olist
```

### 3. Build the ChromaDB index

```bash
python scripts/02_build_index.py --all
```

### 4. Set your LLM key and run

```bash
cp .env.example .env
# Add to .env:  GROQ_API_KEY=gsk_...  (free at console.groq.com)

USE_SQLITE=true streamlit run src/app/app.py
# Open http://localhost:8501
```

---

## LLM Backend Guide

| Backend | Get key | Notes |
|---|---|---|
| ⚡ **Groq** llama-3.3-70b | [console.groq.com](https://console.groq.com) — free | Recommended: 96.6% accuracy, no cost |
| 🤖 **OpenAI** gpt-4o-mini | [platform.openai.com](https://platform.openai.com) | 96.6% accuracy, ~$0.03/run |
| 🤗 **HuggingFace** Inference | [hf.co/settings/tokens](https://huggingface.co/settings/tokens) — free tier | Good free option |
| 🖥️ **Ollama** (local) | [ollama.ai](https://ollama.ai) — no key needed | `ollama pull llama3.2:3b` |

---

## Running the Benchmark

```bash
# Groq (free, fast)
USE_SQLITE=true USE_GROQ=1 GROQ_MODEL=llama-3.3-70b-versatile \
  python eval/evaluate.py

# OpenAI
USE_SQLITE=true OPENAI_API_KEY=sk-... \
  python eval/evaluate.py

# Results saved to eval/report.csv
```

---

## Testing

```bash
pytest tests/ -v
# 77 tests: guardrails, prompt builder, eval utilities
```

---

## Repo Structure

```
NL2SQL/
├── src/
│   ├── app/app.py              # Streamlit UI
│   ├── rag/                    # ChromaDB retrieval pipeline
│   │   ├── build_index.py
│   │   ├── chunk_schema.py
│   │   └── retrieve.py
│   ├── t2sql/                  # SQL generation pipeline
│   │   ├── prompt_builder.py   # Schema RAG → prompt
│   │   ├── generate.py         # Groq / OpenAI / HF / Ollama
│   │   ├── guardrails.py       # sqlglot AST validation
│   │   └── executor.py
│   └── db/
│       └── sqlite_connect.py
├── scripts/
│   ├── build_databases.py      # Download + build SQLite DBs
│   └── 02_build_index.py       # Build ChromaDB vector index
├── data/
│   ├── schemas/                # JSON schema definitions (3 domains)
│   └── examples/               # Few-shot SQL examples (3 domains)
├── eval/
│   ├── gold.jsonl              # 59 gold queries
│   ├── evaluate.py             # Benchmark runner
│   └── demo_questions.json     # Example questions for UI
├── tests/                      # 77 pytest unit tests
├── Dockerfile                  # Local dev (port 8501)
├── spaces.Dockerfile           # HF Spaces deploy (port 7860)
└── spaces_README.md            # HF Spaces README
```

---

## License

MIT
