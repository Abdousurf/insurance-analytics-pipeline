"""
Insurance Analytics Dashboard
==============================
Streamlit dashboard consuming dbt marts from DuckDB.
Displays: Loss Ratio, Claims Frequency, Severity, IBNR alerts.
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "warehouse.duckdb"

st.set_page_config(
    page_title="Insurance Analytics",
    page_icon="📊",
    layout="wide",
)

@st.cache_data(ttl=300)
def load_mart(table: str) -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    df = con.execute(f"SELECT * FROM {table}").df()
    con.close()
    return df


def kpi_card(col, title: str, value: str, delta: str = None, color: str = "#1f77b4"):
    col.markdown(f"""
    <div style="background:{color}15; border-left:4px solid {color};
                padding:16px; border-radius:8px; margin-bottom:8px">
        <div style="font-size:12px; color:#666; margin-bottom:4px">{title}</div>
        <div style="font-size:28px; font-weight:700; color:#222">{value}</div>
        {"<div style='font-size:12px; color:#888'>"+delta+"</div>" if delta else ""}
    </div>
    """, unsafe_allow_html=True)


# ── Header ────────────────────────────────────────────────────────────────
st.title("📊 Insurance Claims Analytics")
st.caption("P&C Portfolio — End-to-end Analytics Engineering Pipeline")

# ── Sidebar filters ────────────────────────────────────────────────────────
st.sidebar.header("Filters")
df = load_mart("mart_loss_ratio")

lob_options = ["All"] + sorted(df["line_of_business"].unique().tolist())
year_options = sorted(df["accident_year"].unique().tolist(), reverse=True)

selected_lob = st.sidebar.selectbox("Line of Business", lob_options)
selected_year = st.sidebar.multiselect("Accident Year", year_options, default=year_options[:2])
show_alerts = st.sidebar.toggle("Show High S/P Alerts Only", value=False)

# ── Apply filters ──────────────────────────────────────────────────────────
filtered = df.copy()
if selected_lob != "All":
    filtered = filtered[filtered["line_of_business"] == selected_lob]
if selected_year:
    filtered = filtered[filtered["accident_year"].isin(selected_year)]
if show_alerts:
    filtered = filtered[filtered["high_loss_ratio_flag"] == True]

# ── KPI Row ────────────────────────────────────────────────────────────────
st.subheader("Portfolio KPIs")
k1, k2, k3, k4, k5 = st.columns(5)

total_premium = filtered["earned_premium_eur"].sum()
total_incurred = filtered["incurred_losses_eur"].sum()
overall_lr = total_incurred / total_premium if total_premium > 0 else 0
total_claims = filtered["claim_count"].sum()
total_policies = filtered["policy_count"].sum()
overall_freq = total_claims / total_policies if total_policies > 0 else 0
avg_sev = total_incurred / total_claims if total_claims > 0 else 0
ibnr_claims = filtered["ibnr_claims_count"].sum()
ibnr_rate = ibnr_claims / total_claims if total_claims > 0 else 0

lr_color = "#e74c3c" if overall_lr > 0.85 else "#27ae60" if overall_lr < 0.70 else "#f39c12"

kpi_card(k1, "Loss Ratio (S/P)", f"{overall_lr:.1%}", color=lr_color)
kpi_card(k2, "Earned Premium", f"€{total_premium/1e6:.1f}M", color="#3498db")
kpi_card(k3, "Incurred Losses", f"€{total_incurred/1e6:.1f}M", color="#e67e22")
kpi_card(k4, "Claims Frequency", f"{overall_freq:.2%}", color="#9b59b6")
kpi_card(k5, "IBNR Rate", f"{ibnr_rate:.1%}", color="#1abc9c")

st.divider()

# ── Charts ─────────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Loss Ratio by Line of Business")
    lob_agg = (
        filtered.groupby("line_of_business")
        .agg(earned=("earned_premium_eur", "sum"), incurred=("incurred_losses_eur", "sum"))
        .assign(loss_ratio=lambda d: d["incurred"] / d["earned"])
        .reset_index()
        .sort_values("loss_ratio", ascending=True)
    )
    fig = px.bar(
        lob_agg, x="loss_ratio", y="line_of_business",
        orientation="h", text_auto=".1%",
        color="loss_ratio",
        color_continuous_scale=["#27ae60", "#f39c12", "#e74c3c"],
        range_color=[0.5, 1.0],
    )
    fig.add_vline(x=0.75, line_dash="dash", line_color="gray", annotation_text="Target 75%")
    fig.update_layout(showlegend=False, coloraxis_showscale=False, height=300)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Claims Frequency by Region")
    reg_agg = (
        filtered.groupby("region")
        .agg(claims=("claim_count", "sum"), policies=("policy_count", "sum"))
        .assign(freq=lambda d: d["claims"] / d["policies"])
        .reset_index()
        .sort_values("freq", ascending=False)
    )
    fig2 = px.bar(
        reg_agg, x="region", y="freq", text_auto=".2%",
        color="freq", color_continuous_scale="RdYlGn_r"
    )
    fig2.update_layout(coloraxis_showscale=False, height=300,
                       xaxis_tickangle=-30)
    st.plotly_chart(fig2, use_container_width=True)

# ── Loss Ratio Trend ────────────────────────────────────────────────────────
st.subheader("Loss Ratio Trend by Year & LOB")
trend = (
    df.groupby(["accident_year", "line_of_business"])
    .agg(ep=("earned_premium_eur", "sum"), il=("incurred_losses_eur", "sum"))
    .assign(lr=lambda d: d["il"] / d["ep"])
    .reset_index()
)
fig3 = px.line(
    trend, x="accident_year", y="lr",
    color="line_of_business", markers=True,
    labels={"lr": "Loss Ratio", "accident_year": "Year"}
)
fig3.add_hline(y=0.75, line_dash="dot", line_color="gray", annotation_text="Target")
fig3.update_yaxes(tickformat=".0%")
st.plotly_chart(fig3, use_container_width=True)

# ── Alerts table ────────────────────────────────────────────────────────────
alerts = df[df["high_loss_ratio_flag"] == True].sort_values("loss_ratio", ascending=False)
if not alerts.empty:
    st.subheader(f"⚠️ High Loss Ratio Alerts ({len(alerts)} segments)")
    st.dataframe(
        alerts[["line_of_business", "region", "accident_year",
                "loss_ratio", "earned_premium_eur", "claim_count"]]
        .style.format({
            "loss_ratio": "{:.1%}",
            "earned_premium_eur": "€{:,.0f}",
        }),
        use_container_width=True,
    )
