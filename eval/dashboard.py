# eval/dashboard.py
"""
Evaluation Dashboard — reads eval/compare_report.csv and visualises
model-vs-model accuracy, latency, cost, and per-query breakdown.

Run:
    streamlit run eval/dashboard.py
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

# ── optional plotly (falls back to st.bar_chart if missing) ──────────────────
try:
    import plotly.express as px
    import plotly.graph_objects as go
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

REPORT_PATH = Path("eval/compare_report.csv")
SUMMARY_PATH = Path("eval/compare_summary.csv")

MODEL_COLORS = {
    "ollama": "#4f8ef7",
    "gpt-5.4-mini": "#f97316",
}

st.set_page_config(
    page_title="NL→SQL Eval Dashboard",
    page_icon="📊",
    layout="wide",
)

# ── Load data ─────────────────────────────────────────────────────────────────
@st.cache_data
def load_data():
    if not REPORT_PATH.exists():
        return None, None
    df = pd.read_csv(REPORT_PATH)
    df["correct"] = df["status"] == "OK"
    summary = None
    if SUMMARY_PATH.exists():
        summary = pd.read_csv(SUMMARY_PATH)
    return df, summary


df, summary_df = load_data()

st.title("📊 NL→SQL Evaluation Dashboard")
st.caption("Model comparison · Accuracy · Latency · Cost · Per-query breakdown")

if df is None:
    st.error(
        f"No report found at `{REPORT_PATH}`. "
        "Run `python -m eval.evaluate_compare` first."
    )
    st.stop()

# ── Sidebar filters ───────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")
    all_models = sorted(df["model"].unique())
    sel_models = st.multiselect("Models", all_models, default=all_models)

    all_domains = sorted(df["domain"].unique())
    sel_domains = st.multiselect("Domains", all_domains, default=all_domains)

    show_failures_only = st.toggle("Show failures only", value=False)

    st.divider()
    st.markdown("**Report file**")
    st.code(str(REPORT_PATH.resolve()), language=None)

filtered = df[df["model"].isin(sel_models) & df["domain"].isin(sel_domains)]
if show_failures_only:
    filtered = filtered[~filtered["correct"]]

# ── Headline metrics ──────────────────────────────────────────────────────────
st.subheader("Headline Numbers")
cols = st.columns(len(all_models) * 2 + 1)

col_idx = 0
for model in all_models:
    grp = df[df["model"] == model]
    acc = grp["correct"].mean() * 100
    avg_lat = grp["latency_s"].mean()
    total_cost = grp["cost_usd"].sum()
    cost_str = f"${total_cost:.4f}" if total_cost > 0 else "$0.00"

    cols[col_idx].metric(
        label=f"✅ Accuracy — {model}",
        value=f"{acc:.1f}%",
        delta=f"{grp['correct'].sum()}/{len(grp)} queries",
    )
    col_idx += 1
    cols[col_idx].metric(
        label=f"⚡ Avg Latency — {model}",
        value=f"{avg_lat:.2f}s",
        delta="per query",
    )
    col_idx += 1

# Cost comparison in last column
gpt_cost = df[df["model"] == "gpt-5.4-mini"]["cost_usd"].sum() if "gpt-5.4-mini" in all_models else 0
cols[col_idx].metric(
    label="💰 Total API Cost",
    value=f"${gpt_cost:.4f}",
    delta="vs $0 for local Ollama",
)

st.divider()

# ── Row 1: Accuracy by domain  |  Accuracy comparison bar ────────────────────
row1_left, row1_right = st.columns(2)

with row1_left:
    st.subheader("Accuracy by Domain")
    domain_acc = (
        df[df["model"].isin(sel_models)]
        .groupby(["model", "domain"])["correct"]
        .agg(["sum", "count"])
        .reset_index()
    )
    domain_acc["accuracy_%"] = domain_acc["sum"] / domain_acc["count"] * 100

    if HAS_PLOTLY:
        fig = px.bar(
            domain_acc,
            x="domain",
            y="accuracy_%",
            color="model",
            barmode="group",
            color_discrete_map=MODEL_COLORS,
            range_y=[0, 105],
            labels={"accuracy_%": "Accuracy (%)", "domain": "Domain", "model": "Model"},
            text=domain_acc["accuracy_%"].apply(lambda v: f"{v:.0f}%"),
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(
            margin=dict(t=20, b=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            height=340,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        pivot = domain_acc.pivot(index="domain", columns="model", values="accuracy_%")
        st.bar_chart(pivot)

with row1_right:
    st.subheader("Overall Accuracy")
    overall = (
        df[df["model"].isin(sel_models)]
        .groupby("model")["correct"]
        .agg(["sum", "count"])
        .reset_index()
    )
    overall["accuracy_%"] = overall["sum"] / overall["count"] * 100
    overall["label"] = overall.apply(
        lambda r: f"{r['sum']:.0f}/{r['count']:.0f}  ({r['accuracy_%']:.1f}%)", axis=1
    )

    if HAS_PLOTLY:
        fig2 = px.bar(
            overall,
            x="model",
            y="accuracy_%",
            color="model",
            color_discrete_map=MODEL_COLORS,
            range_y=[0, 105],
            text="label",
            labels={"accuracy_%": "Accuracy (%)", "model": "Model"},
        )
        fig2.update_traces(textposition="outside")
        fig2.update_layout(
            showlegend=False,
            margin=dict(t=20, b=20),
            height=340,
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.bar_chart(overall.set_index("model")["accuracy_%"])

st.divider()

# ── Row 2: Latency distribution  |  Cost breakdown ───────────────────────────
row2_left, row2_right = st.columns(2)

with row2_left:
    st.subheader("Latency Distribution")
    lat_data = df[df["model"].isin(sel_models)]

    if HAS_PLOTLY:
        fig3 = px.box(
            lat_data,
            x="model",
            y="latency_s",
            color="model",
            color_discrete_map=MODEL_COLORS,
            points="all",
            labels={"latency_s": "Latency (s)", "model": "Model"},
        )
        fig3.update_layout(
            showlegend=False,
            margin=dict(t=20, b=20),
            height=340,
        )
        st.plotly_chart(fig3, use_container_width=True)
    else:
        lat_pivot = lat_data.pivot_table(index="question", columns="model", values="latency_s")
        st.bar_chart(lat_pivot.mean())

with row2_right:
    st.subheader("Cost & Token Usage")
    if "gpt-5.4-mini" in sel_models and df["cost_usd"].sum() > 0:
        gpt_df = df[df["model"] == "gpt-5.4-mini"]
        cost_by_domain = gpt_df.groupby("domain")["cost_usd"].sum().reset_index()
        cost_by_domain.columns = ["domain", "cost_usd"]

        if HAS_PLOTLY:
            fig4 = px.pie(
                cost_by_domain,
                names="domain",
                values="cost_usd",
                title=f"GPT-5.4-mini cost by domain  (total: ${gpt_df['cost_usd'].sum():.4f})",
                hole=0.4,
            )
            fig4.update_layout(margin=dict(t=40, b=20), height=340)
            st.plotly_chart(fig4, use_container_width=True)
        else:
            st.bar_chart(cost_by_domain.set_index("domain")["cost_usd"])

        tok_col1, tok_col2 = st.columns(2)
        tok_col1.metric("Total input tokens", f"{int(gpt_df['input_tokens'].sum()):,}")
        tok_col2.metric("Total output tokens", f"{int(gpt_df['output_tokens'].sum()):,}")
    else:
        st.info("Cost breakdown only available for gpt-5.4-mini. Select it in the sidebar.")

st.divider()

# ── Summary table ─────────────────────────────────────────────────────────────
st.subheader("Model Summary Table")
if summary_df is not None:
    display_summary = summary_df.copy()
    display_summary["total_cost_usd"] = display_summary["total_cost_usd"].apply(
        lambda v: f"${v:.4f}" if v > 0 else "$0.00 (local)"
    )
    display_summary["accuracy_%"] = display_summary["accuracy_%"].apply(lambda v: f"{v:.2f}%")
    display_summary["avg_latency_s"] = display_summary["avg_latency_s"].apply(lambda v: f"{v:.3f}s")
    st.dataframe(display_summary, use_container_width=True, hide_index=True)
else:
    agg = (
        df[df["model"].isin(sel_models)]
        .groupby("model")
        .agg(
            queries=("correct", "count"),
            correct=("correct", "sum"),
            avg_latency_s=("latency_s", "mean"),
            total_cost_usd=("cost_usd", "sum"),
        )
        .reset_index()
    )
    agg["accuracy_%"] = (agg["correct"] / agg["queries"] * 100).round(2)
    st.dataframe(agg, use_container_width=True, hide_index=True)

st.divider()

# ── Per-query detail ──────────────────────────────────────────────────────────
st.subheader("Per-Query Results")

def highlight_row(row):
    if row["status"] != "OK":
        return ["background-color: #fff0f0; color: #c0392b"] * len(row)
    return [""] * len(row)


display_cols = ["model", "domain", "question", "status", "latency_s", "cost_usd", "pred_rows", "gold_rows"]
display_df = filtered[display_cols].copy()
display_df["latency_s"] = display_df["latency_s"].round(3)
display_df["cost_usd"] = display_df["cost_usd"].apply(lambda v: f"${v:.6f}" if v > 0 else "$0.00")

styled = display_df.style.apply(highlight_row, axis=1)
st.dataframe(styled, use_container_width=True, hide_index=True, height=420)

# ── Failure detail expander ────────────────────────────────────────────────────
failures = filtered[filtered["status"] != "OK"]
if len(failures) > 0:
    st.subheader(f"❌ Failure Cases ({len(failures)})")
    for _, row in failures.iterrows():
        with st.expander(f"{row['model']} | {row['domain']} | {row['question']}"):
            st.markdown(f"**Status:** `{row['status']}`")
            if pd.notna(row.get("error")) and row["error"]:
                st.markdown("**Error:**")
                st.code(row["error"])
            if pd.notna(row.get("pred_sql_safe")) and row["pred_sql_safe"]:
                st.markdown("**Generated SQL:**")
                st.code(row["pred_sql_safe"], language="sql")
            if pd.notna(row.get("gold_sql_safe")) and row["gold_sql_safe"]:
                st.markdown("**Gold SQL:**")
                st.code(row["gold_sql_safe"], language="sql")
else:
    st.success("✅ No failures in the current filter selection.")

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "Data: `eval/compare_report.csv` · "
    "Re-run benchmark: `python -m eval.evaluate_compare` · "
    "Dashboard: `streamlit run eval/dashboard.py`"
)
