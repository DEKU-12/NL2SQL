# eval/eval_utils.py
"""
Pure utility functions for evaluation — no DB, no LLM, no heavy deps.
Importable standalone for unit testing.
"""
from __future__ import annotations

import pandas as pd

ROUND_DECIMALS = 2

# GPT-5.4-mini pricing (per 1M tokens, March 2026)
GPT54_MINI_INPUT_PRICE_PER_1M  = 0.750
GPT54_MINI_OUTPUT_PRICE_PER_1M = 4.500


def estimate_cost(input_tokens: int, output_tokens: int) -> float:
    """Estimate USD cost for a GPT-5.4-mini API call."""
    return (
        input_tokens  / 1_000_000 * GPT54_MINI_INPUT_PRICE_PER_1M
        + output_tokens / 1_000_000 * GPT54_MINI_OUTPUT_PRICE_PER_1M
    )


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise a result DataFrame for stable comparison:
    - lowercase column names
    - sort columns alphabetically
    - round numeric columns to ROUND_DECIMALS places
    - cast everything to string
    - sort rows lexicographically
    """
    df2 = df.copy()
    df2.columns = [str(c).lower() for c in df2.columns]
    df2 = df2.sort_index(axis=1)
    for c in df2.columns:
        try:
            if pd.api.types.is_numeric_dtype(df2[c]):
                df2[c] = df2[c].round(ROUND_DECIMALS)
        except Exception:
            pass
    df2 = df2.astype(str)
    if len(df2.columns) > 0 and len(df2) > 0:
        df2 = df2.sort_values(list(df2.columns)).reset_index(drop=True)
    else:
        df2 = df2.reset_index(drop=True)
    return df2


def same_result(df_pred: pd.DataFrame, df_gold: pd.DataFrame) -> bool:
    """
    Fair equivalence check between two result DataFrames:
    - 1×1 scalar: compare as float (rounded)
    - else: compare on common columns only, up to min(rows_pred, rows_gold)
    """
    try:
        if df_pred.shape == (1, 1) and df_gold.shape == (1, 1):
            a, b = df_pred.iloc[0, 0], df_gold.iloc[0, 0]
            try:
                return round(float(a), ROUND_DECIMALS) == round(float(b), ROUND_DECIMALS)
            except Exception:
                return str(a).strip() == str(b).strip()

        a = normalize_df(df_pred)
        b = normalize_df(df_gold)
        common_cols = [c for c in a.columns if c in b.columns]
        if not common_cols:
            return False
        a2 = a[common_cols].reset_index(drop=True)
        b2 = b[common_cols].reset_index(drop=True)
        n = min(len(a2), len(b2))
        return a2.head(n).reset_index(drop=True).equals(b2.head(n).reset_index(drop=True))
    except Exception:
        return False
