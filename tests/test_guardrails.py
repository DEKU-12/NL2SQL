# tests/test_guardrails.py
"""
Unit tests for src/t2sql/guardrails.py

Tests cover:
  - strip_code_fences  : markdown fence removal
  - is_select_only     : allowlist / blocklist logic
  - enforce_limit      : LIMIT injection and clamping
  - validate_and_fix   : end-to-end pipeline
"""
import pytest
from src.t2sql.guardrails import (
    strip_code_fences,
    is_select_only,
    enforce_limit,
    validate_and_fix,
)


# ── strip_code_fences ─────────────────────────────────────────────────────────

class TestStripCodeFences:
    def test_removes_sql_fence(self):
        sql = "```sql\nSELECT 1;\n```"
        assert strip_code_fences(sql) == "SELECT 1;"

    def test_removes_plain_fence(self):
        sql = "```\nSELECT 1;\n```"
        assert strip_code_fences(sql) == "SELECT 1;"

    def test_passthrough_when_no_fence(self):
        sql = "SELECT id FROM users LIMIT 10;"
        assert strip_code_fences(sql) == sql

    def test_strips_surrounding_whitespace(self):
        sql = "  ```sql\nSELECT 1\n```  "
        assert strip_code_fences(sql) == "SELECT 1"


# ── is_select_only ────────────────────────────────────────────────────────────

class TestIsSelectOnly:
    def test_plain_select_passes(self):
        assert is_select_only("SELECT id FROM users LIMIT 10;") is True

    def test_with_cte_passes(self):
        sql = "WITH t AS (SELECT 1 AS x) SELECT x FROM t LIMIT 5;"
        assert is_select_only(sql) is True

    def test_insert_blocked(self):
        assert is_select_only("INSERT INTO users VALUES (1);") is False

    def test_update_blocked(self):
        assert is_select_only("UPDATE users SET name='x' WHERE id=1;") is False

    def test_delete_blocked(self):
        assert is_select_only("DELETE FROM users WHERE id=1;") is False

    def test_drop_blocked(self):
        assert is_select_only("DROP TABLE users;") is False

    def test_multiple_statements_blocked(self):
        # semicolon in the middle = multiple statements
        assert is_select_only("SELECT 1; SELECT 2;") is False

    def test_trailing_semicolon_allowed(self):
        # a single trailing ; is fine
        assert is_select_only("SELECT id FROM users LIMIT 10;") is True

    def test_case_insensitive_block(self):
        assert is_select_only("delete from users where id=1;") is False

    def test_select_with_subquery_passes(self):
        sql = "SELECT * FROM (SELECT id FROM users) sub LIMIT 10;"
        assert is_select_only(sql) is True


# ── enforce_limit ─────────────────────────────────────────────────────────────

class TestEnforceLimit:
    def test_adds_limit_when_missing(self):
        sql = "SELECT id FROM users"
        result = enforce_limit(sql, limit=100)
        assert "LIMIT 100" in result.upper()

    def test_does_not_double_add_limit(self):
        sql = "SELECT id FROM users LIMIT 50"
        result = enforce_limit(sql, limit=100)
        assert result.upper().count("LIMIT") == 1

    def test_clamps_oversized_limit(self):
        sql = "SELECT id FROM users LIMIT 5000"
        result = enforce_limit(sql, limit=200)
        assert "LIMIT 200" in result.upper()
        assert "5000" not in result

    def test_preserves_small_limit(self):
        sql = "SELECT id FROM users LIMIT 5"
        result = enforce_limit(sql, limit=200)
        assert "LIMIT 5" in result.upper()

    def test_removes_trailing_semicolon_before_adding_limit(self):
        sql = "SELECT id FROM users;"
        result = enforce_limit(sql, limit=50)
        # Should not have ;LIMIT or duplicate semicolons
        assert result.count(";") <= 1
        assert "LIMIT 50" in result.upper()


# ── validate_and_fix ──────────────────────────────────────────────────────────

class TestValidateAndFix:
    def test_valid_select_passes(self):
        sql = "SELECT id FROM users"
        result = validate_and_fix(sql, limit=100)
        assert result is not None
        assert "LIMIT" in result.upper()

    def test_strips_fence_and_validates(self):
        sql = "```sql\nSELECT id FROM users\n```"
        result = validate_and_fix(sql, limit=100)
        assert "SELECT" in result.upper()
        assert "```" not in result

    def test_raises_on_dml(self):
        with pytest.raises(ValueError, match="Guardrails"):
            validate_and_fix("DELETE FROM users;", limit=100)

    def test_raises_on_ddl(self):
        with pytest.raises(ValueError, match="Guardrails"):
            validate_and_fix("DROP TABLE users;", limit=100)

    def test_raises_on_multiple_statements(self):
        with pytest.raises(ValueError, match="Guardrails"):
            validate_and_fix("SELECT 1; DROP TABLE users;", limit=100)

    def test_clamps_limit_end_to_end(self):
        sql = "SELECT id FROM users LIMIT 9999"
        result = validate_and_fix(sql, limit=200)
        assert "LIMIT 200" in result.upper()
        assert "9999" not in result
