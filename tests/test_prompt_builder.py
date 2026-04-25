# tests/test_prompt_builder.py
"""
Unit tests for src/t2sql/prompt_builder.py

Tests cover:
  - detect_top_n   : regex extraction of N from natural-language questions
  - looks_scalar   : aggregate question detection
  - domain_glossary: correct glossary returned per domain
  - build_prompt   : prompt contains required sections
"""
import pytest
from src.t2sql.prompt_builder import (
    detect_top_n,
    looks_scalar,
    domain_glossary,
    build_prompt,
)


# ── detect_top_n ──────────────────────────────────────────────────────────────

class TestDetectTopN:
    def test_detects_top_5(self):
        assert detect_top_n("top 5 customers by spend") == 5

    def test_detects_top_10(self):
        assert detect_top_n("top 10 artists by sales") == 10

    def test_returns_none_when_absent(self):
        assert detect_top_n("average invoice total") is None

    def test_case_insensitive(self):
        assert detect_top_n("TOP 3 products by revenue") == 3

    def test_ignores_non_top_numbers(self):
        # "3 domains" should not be treated as top-N
        assert detect_top_n("list 3 domains") is None


# ── looks_scalar ──────────────────────────────────────────────────────────────

class TestLooksScalar:
    def test_average_is_scalar(self):
        assert looks_scalar("average invoice total") is True

    def test_total_is_scalar(self):
        assert looks_scalar("total revenue overall") is True

    def test_count_is_scalar(self):
        assert looks_scalar("number of distinct customers") is True

    def test_top_n_is_not_scalar(self):
        # "top N" questions return multiple rows, not a single aggregate
        assert looks_scalar("top 5 customers by spend") is False

    def test_listing_question_is_not_scalar(self):
        assert looks_scalar("films never rented") is False


# ── domain_glossary ───────────────────────────────────────────────────────────

class TestDomainGlossary:
    def test_chinook_mentions_invoice(self):
        g = domain_glossary("chinook")
        assert "invoice" in g.lower()

    def test_dvdrental_mentions_payment(self):
        g = domain_glossary("dvdrental")
        assert "payment" in g.lower()

    def test_northwind_mentions_discount(self):
        g = domain_glossary("northwind")
        assert "discount" in g.lower()

    def test_unknown_domain_returns_empty(self):
        assert domain_glossary("unknown_db") == ""

    def test_case_insensitive_domain(self):
        # "Chinook" and "chinook" should return the same glossary
        assert domain_glossary("Chinook") == domain_glossary("chinook")


# ── build_prompt ──────────────────────────────────────────────────────────────

class TestBuildPrompt:
    def _make_chunks(self):
        return [{"text": "TABLE customer (customer_id INT, first_name TEXT)"}]

    def test_prompt_contains_question(self):
        prompt = build_prompt("chinook", "top 5 customers by spend", self._make_chunks())
        assert "top 5 customers by spend" in prompt

    def test_prompt_contains_schema_context(self):
        prompt = build_prompt("chinook", "top 5 customers by spend", self._make_chunks())
        assert "customer_id" in prompt

    def test_prompt_contains_domain(self):
        prompt = build_prompt("chinook", "any question", self._make_chunks())
        assert "chinook" in prompt.lower()

    def test_prompt_contains_sql_dialect(self):
        prompt = build_prompt("chinook", "any question", self._make_chunks(), dialect="PostgreSQL")
        assert "PostgreSQL" in prompt

    def test_empty_chunks_handled_gracefully(self):
        prompt = build_prompt("chinook", "some question", chunks=[])
        assert "no schema context found" in prompt

    def test_prompt_is_string(self):
        prompt = build_prompt("northwind", "total revenue", self._make_chunks())
        assert isinstance(prompt, str) and len(prompt) > 0
