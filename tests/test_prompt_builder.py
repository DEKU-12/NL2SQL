# tests/test_prompt_builder.py
"""
Unit tests for src/t2sql/prompt_builder.py

Tests cover:
  - detect_top_n   : regex extraction of N from natural-language questions
  - looks_scalar   : aggregate question detection (incl. GROUP-BY exclusion)
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
        assert detect_top_n("top 5 complaint types by count") == 5

    def test_detects_top_10(self):
        assert detect_top_n("top 10 sellers by revenue") == 10

    def test_returns_none_when_absent(self):
        assert detect_top_n("average resolution time in days") is None

    def test_case_insensitive(self):
        assert detect_top_n("TOP 3 boroughs by complaints") == 3

    def test_ignores_non_top_numbers(self):
        # "3 domains" should not be treated as top-N
        assert detect_top_n("list 3 domains") is None


# ── looks_scalar ──────────────────────────────────────────────────────────────

class TestLooksScalar:
    # --- should be True (genuine single-value questions) ---
    def test_average_overall_is_scalar(self):
        assert looks_scalar("What is the average resolution time?") is True

    def test_total_overall_is_scalar(self):
        assert looks_scalar("What is the total revenue in 2018?") is True

    def test_count_is_scalar(self):
        # "number of" is a scalar marker; "how many" is not — use the right phrasing
        assert looks_scalar("What is the number of unique complaint types?") is True

    def test_max_is_scalar(self):
        assert looks_scalar("What is the maximum claim cost?") is True

    def test_min_is_scalar(self):
        assert looks_scalar("What is the minimum delivery time?") is True

    # --- should be False (GROUP BY / multi-row questions) ---
    def test_top_n_is_not_scalar(self):
        assert looks_scalar("top 5 complaint types by count") is False

    def test_average_by_group_is_not_scalar(self):
        # "average X by Y" → GROUP BY result, not scalar
        assert looks_scalar("What is the average resolution time by borough?") is False

    def test_total_by_group_is_not_scalar(self):
        assert looks_scalar("Show total revenue by payment type") is False

    def test_count_by_group_is_not_scalar(self):
        assert looks_scalar("Show the number of complaints by agency") is False

    def test_average_by_state_is_not_scalar(self):
        assert looks_scalar("What is the average freight value by seller state?") is False

    def test_listing_question_is_not_scalar(self):
        assert looks_scalar("Which boroughs had noise complaints?") is False


# ── domain_glossary ───────────────────────────────────────────────────────────

class TestDomainGlossary:
    def test_nyc_311_mentions_borough(self):
        g = domain_glossary("nyc_311")
        assert "borough" in g.lower()

    def test_nyc_311_mentions_strftime(self):
        g = domain_glossary("nyc_311")
        assert "strftime" in g.lower()

    def test_olist_mentions_revenue(self):
        g = domain_glossary("olist_ecommerce")
        assert "revenue" in g.lower()

    def test_olist_mentions_join(self):
        g = domain_glossary("olist_ecommerce")
        assert "join" in g.lower()

    def test_synthea_mentions_patient(self):
        g = domain_glossary("synthea_patients")
        assert "patient" in g.lower()

    def test_synthea_mentions_encounter(self):
        g = domain_glossary("synthea_patients")
        assert "encounter" in g.lower()

    def test_unknown_domain_returns_empty(self):
        assert domain_glossary("unknown_db") == ""

    def test_case_insensitive_domain(self):
        # "NYC_311" and "nyc_311" should return the same glossary
        assert domain_glossary("NYC_311") == domain_glossary("nyc_311")


# ── build_prompt ──────────────────────────────────────────────────────────────

class TestBuildPrompt:
    def _make_chunks(self):
        return [{"text": "TABLE nyc_311 (unique_key TEXT, complaint_type TEXT, borough TEXT)"}]

    def test_prompt_contains_question(self):
        prompt = build_prompt("nyc_311", "top 5 complaint types by count", self._make_chunks())
        assert "top 5 complaint types by count" in prompt

    def test_prompt_contains_schema_context(self):
        prompt = build_prompt("nyc_311", "top 5 complaint types", self._make_chunks())
        assert "complaint_type" in prompt

    def test_prompt_contains_domain(self):
        prompt = build_prompt("nyc_311", "any question", self._make_chunks())
        assert "nyc_311" in prompt.lower()

    def test_prompt_contains_sql_dialect_postgresql(self):
        prompt = build_prompt("nyc_311", "any question", self._make_chunks(), dialect="PostgreSQL")
        assert "PostgreSQL" in prompt

    def test_prompt_contains_sql_dialect_sqlite(self):
        prompt = build_prompt("nyc_311", "any question", self._make_chunks(), dialect="SQLite")
        assert "SQLite" in prompt

    def test_empty_chunks_handled_gracefully(self):
        prompt = build_prompt("nyc_311", "some question", chunks=[])
        assert "no schema context found" in prompt

    def test_prompt_is_string(self):
        prompt = build_prompt("olist_ecommerce", "total revenue", self._make_chunks())
        assert isinstance(prompt, str) and len(prompt) > 0

    def test_scalar_hint_injected_for_aggregate_question(self):
        prompt = build_prompt("synthea_patients", "What is the average healthcare cost?", self._make_chunks())
        assert "ONE row" in prompt

    def test_scalar_hint_not_injected_for_group_by_question(self):
        # "by borough" triggers the GROUP BY exclusion — the guidance tip should not appear
        # (SYSTEM_RULES contains "ONE" in other contexts so we check the specific tip string)
        prompt = build_prompt("nyc_311", "average resolution time by borough", self._make_chunks())
        assert "single-value question" not in prompt

    def test_top_n_limit_hint_injected(self):
        prompt = build_prompt("nyc_311", "top 3 complaint types", self._make_chunks())
        assert "LIMIT 3" in prompt
