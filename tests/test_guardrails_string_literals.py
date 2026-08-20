import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from t2sql.guardrails import is_select_only, validate_and_fix


def test_forbidden_keyword_inside_string_literal_is_allowed():
    sql = "SELECT * FROM audit WHERE action = 'delete'"
    assert is_select_only(sql) is True


def test_semicolon_inside_string_literal_is_allowed():
    sql = "SELECT * FROM audit WHERE note = 'a; b'"
    assert is_select_only(sql) is True


def test_actual_forbidden_keyword_outside_string_is_rejected():
    sql = "DELETE FROM audit WHERE id = 1"
    assert is_select_only(sql) is False


def test_validate_and_fix_allows_string_literal_with_keyword():
    sql = "SELECT * FROM audit WHERE action = 'delete'"
