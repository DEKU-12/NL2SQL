<div align="center">

# 🧠 NL → SQL Copilot

**Ask your database a question in plain English. Get safe, executable SQL — and the real rows back.**

Schema-aware RAG · SQL guardrails · bring-your-own LLM · three real-world databases (3.0M rows)

[![Live Demo](https://img.shields.io/badge/🤗%20Live%20Demo-Hugging%20Face%20Spaces-blue)](https://huggingface.co/spaces/DEKU02/nl2sql)
[![CI](https://github.com/DEKU-12/NL2SQL/actions/workflows/ci.yml/badge.svg)](https://github.com/DEKU-12/NL2SQL/actions/workflows/ci.yml)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](#license)

### [▶ Try it live → huggingface.co/spaces/DEKU02/nl2sql](https://huggingface.co/spaces/DEKU02/nl2sql)

</div>

---

## 🎬 Demo

<!-- DEMO VIDEO: edit this file on GitHub, delete the line below, and drag nl2sql-demo.mp4 into its place. GitHub uploads it and inserts a link that plays inline. -->
*Demo video coming here — 40 seconds of the real app: question → SQL → results.*

---

## Contents

- [Highlights](#-highlights)
- [Benchmark](#-benchmark)
- [Databases](#-databases)
- [How it works](#-how-it-works)
- [Quick start](#-quick-start)
- [Configuration](#-configuration)
- [Evaluation](#-evaluation)
- [Deployment](#-deployment)
- [Project structure](#-project-structure)
- [Limitations & roadmap](#-limitations--roadmap)

---

## ✨ Highlights

- **Plain English → SQL → results** in one click: type a question, generate the query, run it, and download the result table as CSV.
- **Three real databases** — NYC 311 service requests, Olist Brazilian e-commerce (8 tables), and Synthea synthetic healthcare records — switchable from the sidebar, each with one-click example questions.
- **Schema-aware RAG** — only the tables a question needs are retrieved from a ChromaDB index, plus **join-path expansion** that pulls in bridge tables along foreign keys (e.g. `order_reviews ↔ orders ↔ order_items`). Chunks carry **real column values** (`borough — values: BROOKLYN, QUEENS, …`) so the model filters on strings that actually exist.
- **SQL guardrails** — every query is parsed with `sqlglot`: a single `SELECT`/`WITH` statement only, no DDL/DML, and a `LIMIT` is always enforced.
- **Bring your own model** — Groq (free, default), Anthropic Claude Opus 5.5, OpenAI, Hugging Face Inference, or a local Ollama model.
- **Bring your own key** — on the live app, visitors paste their own API key, kept only in their session and never saved. Locally, keys are read from your `.env` file and are never deployed.
- **Measured, not claimed** — a 59-query gold benchmark for end-to-end accuracy and a free retrieval-recall check, plus CI and automatic deploys.

---

## 📊 Benchmark

End-to-end execution accuracy on **59 hand-curated gold queries** across all three databases (predicted and gold SQL are both executed and their results compared):

| Model | Accuracy | Cost |
|---|---|---|
| Anthropic Claude Opus 5.5 | **98.3%** (58/59) | Paid |
| OpenAI gpt-4o-mini | **96.6%** (57/59) | ~$0.03 / run |
| Groq llama-3.3-70b-versatile | **96.6%** (57/59) | Free |
| Groq llama-3.1-8b-instant | 94.9% (56/59) | Free |
| Ollama llama3.2:3b (local) | 67.8% (40/59) | Free |

**Retrieval recall** — share of gold queries where every table the gold SQL uses was retrieved (`eval/retrieval_recall.py`, no LLM calls):

| Top-K | Similarity only | + Join-path expansion | Avg. tables in prompt |
|---|---|---|---|
| 2 | 88.1% | 94.9% | 1.9 |
| 3 | 91.5% | 98.3% | 2.7 |
| **6 (default)** | 94.9% | **100%** | 4.1 |

---

## 🗄️ Databases

| Domain | Tables | Rows | Source |
|---|---|---|---|
| 🏙️ **NYC 311** | 1 | 500,000 service requests | [NYC Open Data](https://data.cityofnewyork.us/resource/erm2-nwe9.csv) — auto-download |
| 🛒 **Olist E-Commerce** | 8 | 1.55M (100k+ orders) | [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — needs a free Kaggle token |
| 🏥 **Synthea Healthcare** | 6 | 986k (12k synthetic patients) | [Synthea](https://synthetichealth.github.io/synthea-sample-data/) — auto-download |

---

## ⚙️ How it works

```
 Plain-English question
          │
          ▼
 [1] Schema RAG ─────────── embed question (all-MiniLM-L6-v2) → top-K table chunks from ChromaDB
          │                 + join map (foreign keys) + bridge tables on the FK path
          ▼
 [2] Prompt builder ─────── schema chunks (with real column values) + domain glossary + few-shot examples
          │
          ▼
 [3] LLM ────────────────── Groq · Claude Opus 5.5 · OpenAI · Hugging Face · Ollama
          │
          ▼
 [4] Guardrails ─────────── sqlglot AST: single SELECT/WITH, no DDL/DML, LIMIT enforced
          │
          ▼
 [5] Execute ────────────── SQLite (default, used on Spaces) or PostgreSQL
          │
          ▼
 [6] Results ────────────── table view + CSV download in Streamlit
```

| Layer | Technology |
|---|---|
| UI | Streamlit |
| Vector store | ChromaDB + `sentence-transformers/all-MiniLM-L6-v2` |
| LLM backends | Groq · Anthropic · OpenAI · Hugging Face Inference API · Ollama |
| Guardrails | `sqlglot` AST parsing |
| Databases | SQLite (default) · PostgreSQL (optional) |
| Testing / CI | pytest (77 tests) · GitHub Actions |
| Deployment | Docker · Hugging Face Spaces |

---

## 🚀 Quick start

> Just want to try it? Use the **[live demo](https://huggingface.co/spaces/DEKU02/nl2sql)** — pick a database, paste your API key in the sidebar (a free Groq key works), and ask a question.

### 1. Clone and install

```bash
git clone https://github.com/DEKU-12/NL2SQL.git
cd NL2SQL
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### 2. Build the databases

```bash
# NYC 311 + Synthea download automatically (~450 MB, a few minutes)
python scripts/build_databases.py --only nyc311 synthea

# Olist needs a free Kaggle token: kaggle.com/settings → API → Create New Token
export KAGGLE_API_TOKEN=KGAT_...
python scripts/build_databases.py --only olist
```

### 3. Build the schema index

```bash
python scripts/02_build_index.py --all
```

### 4. Add your API key(s)

```bash
cp .env.example .env
```

Then fill in at least one key in `.env`:

```bash
GROQ_API_KEY=gsk_...          # free at console.groq.com (default backend)
ANTHROPIC_API_KEY=sk-ant-...  # Claude Opus 5.5 — console.anthropic.com
OPENAI_API_KEY=sk-...         # optional
```

### 5. Run

```bash
USE_SQLITE=true streamlit run src/app/app.py
```

Open http://localhost:8501.

### Docker

```bash
docker build -t nl2sql .
docker run -p 8501:8501 nl2sql
```

The image builds the databases and index at build time. `.env` is excluded from the image, so enter your API key in the sidebar.

---

## 🔧 Configuration

| Variable | Default | Purpose |
|---|---|---|
| `USE_SQLITE` | — | `true` to use the bundled SQLite databases (recommended) |
| `GROQ_API_KEY` / `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` / `HF_TOKEN` | — | LLM keys — the app reads them **only from the local `.env` file** |
| `GROQ_MODEL` | `llama-3.3-70b-versatile` | Groq model |
| `ANTHROPIC_MODEL` | `claude-opus-5-5` | Anthropic model |
| `OPENAI_MODEL` | `gpt-4o-mini` | OpenAI model |
| `HF_MODEL` | `Qwen/Qwen2.5-Coder-7B-Instruct` | Hugging Face Inference model |
| `OLLAMA_URL` / `OLLAMA_MODEL` | `http://localhost:11434` / `qwen2.5-coder:7b` | Local Ollama (offered when `USE_SQLITE` is off) |
| `CHROMA_DIR` | `data/chroma` | Schema index location |
| `SQL_MAX_ROWS` | `200` | Row limit enforced on every query |
| `KAGGLE_API_TOKEN` | — | Needed only to download the Olist dataset |
| `POSTGRES_HOST` / `PORT` / `USER` / `PASSWORD` | `localhost` / `5432` / `postgres` / `postgres` | PostgreSQL mode (when `USE_SQLITE` is off) |

---

## 🧪 Evaluation

**End-to-end accuracy** (calls the LLM; keys are read from `.env`):

```bash
USE_SQLITE=true python eval/evaluate.py                          # Groq (default)
USE_SQLITE=true EVAL_BACKEND=anthropic python eval/evaluate.py   # Claude Opus 5.5
USE_SQLITE=true EVAL_BACKEND=openai python eval/evaluate.py      # OpenAI
```

Per-query results are written to `eval/report.csv`.

**Retrieval recall** (free — no LLM calls):

```bash
python eval/retrieval_recall.py
```

**Unit tests:**

```bash
pytest tests/ -v   # 77 tests: guardrails, prompt builder, eval utilities
```

---

## ☁️ Deployment

Every push to `main` runs two GitHub Actions workflows:

- **CI — Tests** (`.github/workflows/ci.yml`): installs dependencies and runs pytest.
- **CD — Deploy to HF Spaces** (`.github/workflows/deploy.yml`): pushes the repo to the [Hugging Face Space](https://huggingface.co/spaces/DEKU02/nl2sql) using `spaces.Dockerfile`, which builds the databases and schema index inside the container.

Setup:

- **GitHub secret `HF_TOKEN`:** a Hugging Face write token, used by the deploy workflow.
- **Space secret `KAGGLE_API_TOKEN` (optional):** enables the Olist database. It's built on the Space's first boot.
- **No LLM keys on the Space:** visitors bring their own.

---

## 📁 Project structure

```
NL2SQL/
├── src/
│   ├── app/app.py                 # Streamlit UI
│   ├── rag/
│   │   ├── chunk_schema.py        # table chunks + real column values
│   │   ├── chunk_relationships.py # foreign-key join map chunk
│   │   ├── build_index.py         # ChromaDB index builder
│   │   └── retrieve.py            # top-K retrieval + join-path expansion
│   ├── t2sql/
│   │   ├── prompt_builder.py      # schema + glossary + few-shot → prompt
│   │   ├── generate.py            # Groq / Anthropic / OpenAI / HF / Ollama
│   │   ├── guardrails.py          # sqlglot validation + LIMIT injection
│   │   └── executor.py            # SQLite / PostgreSQL execution
│   └── db/sqlite_connect.py
├── scripts/
│   ├── build_databases.py         # download + build the SQLite databases
│   └── 02_build_index.py          # build the schema index
├── data/
│   ├── schemas/                   # schema definitions (3 domains)
│   └── examples/                  # few-shot SQL examples (3 domains)
├── eval/
│   ├── gold.jsonl                 # 59 gold queries
│   ├── evaluate.py                # end-to-end accuracy benchmark
│   ├── retrieval_recall.py        # retrieval-only recall check
│   └── demo_questions.json        # example questions for the UI
├── tests/                         # pytest unit tests
├── Dockerfile                     # local container (port 8501)
├── spaces.Dockerfile              # Hugging Face Spaces container (port 7860)
└── .github/workflows/             # CI + deploy
```

---

## 🧭 Limitations & roadmap

- **The benchmark is small and near its ceiling.** With 59 queries, one query is about 1.7 points. Next: 150+ harder queries (multi-join, ambiguous, adversarial) and stricter comparison of result tables.
- **Self-correction isn't used by the app yet.** The retry-on-error loop exists in `generate.py`'s CLI, but the app and eval don't call it.
- **The embedding model reads ~256 tokens per chunk.** Long table descriptions crowd out column details at retrieval time.
- **PostgreSQL mode needs your own loaded databases.** The repo's scripts build SQLite only.

---

## License

MIT
