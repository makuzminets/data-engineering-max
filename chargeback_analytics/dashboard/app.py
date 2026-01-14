"""
Chargeback Analytics Dashboard
Interactive Streamlit dashboard for monitoring payment fraud and chargebacks.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
from datetime import datetime, timedelta

# Page config
st.set_page_config(
    page_title="Chargeback Analytics",
    page_icon="💳",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background-color: #1e1e2e;
        padding: 20px;
        border-radius: 10px;
        border-left: 4px solid #6366f1;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        color: #f8fafc;
    }
    .metric-delta-positive {
        color: #22c55e;
    }
    .metric-delta-negative {
        color: #ef4444;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data
def load_data():
    """Load chargeback data."""
    data_dir = Path(__file__).parent.parent / "data"
    
    transactions = pd.read_csv(data_dir / "transactions.csv", parse_dates=["transaction_date"])
    chargebacks = pd.read_csv(data_dir / "chargebacks.csv", parse_dates=["transaction_date", "chargeback_date", "resolution_date"])
    
    return transactions, chargebacks


def calculate_metrics(transactions: pd.DataFrame, chargebacks: pd.DataFrame, 
                      start_date: datetime, end_date: datetime) -> dict:
    """Calculate key metrics for date range."""
    
    # Filter by date
    tx_filtered = transactions[
        (transactions["transaction_date"] >= pd.Timestamp(start_date)) &
        (transactions["transaction_date"] <= pd.Timestamp(end_date))
    ]
    
    cb_filtered = chargebacks[
        (chargebacks["chargeback_date"] >= pd.Timestamp(start_date)) &
        (chargebacks["chargeback_date"] <= pd.Timestamp(end_date))
    ]
    
    total_transactions = len(tx_filtered)
    total_revenue = tx_filtered["amount"].sum()
    total_chargebacks = len(cb_filtered)
    chargeback_amount = cb_filtered["amount"].sum()
    
    # Calculate rates
    chargeback_rate = (total_chargebacks / total_transactions * 100) if total_transactions > 0 else 0
    chargeback_amount_rate = (chargeback_amount / total_revenue * 100) if total_revenue > 0 else 0
    
    # Dispute metrics
    disputes_filed = cb_filtered["dispute_filed"].sum()
    disputes_won = cb_filtered[cb_filtered["dispute_won"] == True].shape[0]
    dispute_win_rate = (disputes_won / disputes_filed * 100) if disputes_filed > 0 else 0
    
    # Recovery
    recovered = cb_filtered[cb_filtered["dispute_won"] == True]["amount"].sum()
    net_loss = chargeback_amount - recovered
    
    return {
        "total_transactions": total_transactions,
        "total_revenue": total_revenue,
        "total_chargebacks": total_chargebacks,
        "chargeback_amount": chargeback_amount,
        "chargeback_rate": chargeback_rate,
        "chargeback_amount_rate": chargeback_amount_rate,
        "disputes_filed": disputes_filed,
        "disputes_won": disputes_won,
        "dispute_win_rate": dispute_win_rate,
        "recovered": recovered,
        "net_loss": net_loss,
    }


def create_trend_chart(chargebacks: pd.DataFrame, transactions: pd.DataFrame) -> go.Figure:
    """Create chargeback trend chart."""
    
    # Daily aggregation
    cb_daily = chargebacks.groupby(chargebacks["chargeback_date"].dt.date).agg({
        "chargeback_id": "count",
        "amount": "sum"
    }).reset_index()
    cb_daily.columns = ["date", "chargebacks", "amount"]
    
    tx_daily = transactions.groupby(transactions["transaction_date"].dt.date).agg({
        "transaction_id": "count"
    }).reset_index()
    tx_daily.columns = ["date", "transactions"]
    
    # Merge
    daily = tx_daily.merge(cb_daily, on="date", how="left").fillna(0)
    daily["chargeback_rate"] = daily["chargebacks"] / daily["transactions"] * 100
    daily["chargeback_rate_7d"] = daily["chargeback_rate"].rolling(7, min_periods=1).mean()
    
    # Create figure
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        subplot_titles=("Chargeback Rate (%)", "Chargeback Count")
    )
    
    # Rate trend
    fig.add_trace(
        go.Scatter(x=daily["date"], y=daily["chargeback_rate"], 
                   mode="lines", name="Daily Rate", opacity=0.5,
                   line=dict(color="#94a3b8")),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(x=daily["date"], y=daily["chargeback_rate_7d"], 
                   mode="lines", name="7-Day Avg",
                   line=dict(color="#6366f1", width=2)),
        row=1, col=1
    )
    
    # Threshold line
    fig.add_hline(y=1.0, line_dash="dash", line_color="#ef4444", 
                  annotation_text="1% Threshold", row=1, col=1)
    
    # Count trend
    fig.add_trace(
        go.Bar(x=daily["date"], y=daily["chargebacks"], name="Chargebacks",
               marker_color="#6366f1"),
        row=2, col=1
    )
    
    fig.update_layout(
        height=500,
        template="plotly_dark",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02)
    )
    
    return fig


def create_category_chart(chargebacks: pd.DataFrame) -> go.Figure:
    """Create chargeback by category chart."""
    
    category_data = chargebacks.groupby("reason_category").agg({
        "chargeback_id": "count",
        "amount": "sum"
    }).reset_index()
    category_data.columns = ["category", "count", "amount"]
    category_data = category_data.sort_values("count", ascending=True)
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=category_data["category"],
        x=category_data["count"],
        orientation="h",
        marker_color=["#ef4444" if c == "fraud" else "#6366f1" for c in category_data["category"]],
        text=category_data["count"],
        textposition="auto"
    ))
    
    fig.update_layout(
        title="Chargebacks by Category",
        template="plotly_dark",
        height=300,
        xaxis_title="Count",
        yaxis_title=""
    )
    
    return fig


def create_country_chart(chargebacks: pd.DataFrame, transactions: pd.DataFrame) -> go.Figure:
    """Create chargeback by country chart."""
    
    # Calculate rates by country
    cb_country = chargebacks.groupby("country")["chargeback_id"].count().reset_index()
    cb_country.columns = ["country", "chargebacks"]
    
    tx_country = transactions.groupby("country")["transaction_id"].count().reset_index()
    tx_country.columns = ["country", "transactions"]
    
    country_data = tx_country.merge(cb_country, on="country", how="left").fillna(0)
    country_data["rate"] = country_data["chargebacks"] / country_data["transactions"] * 100
    country_data = country_data.sort_values("rate", ascending=True).tail(10)
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=country_data["country"],
        x=country_data["rate"],
        orientation="h",
        marker_color=px.colors.sequential.Reds[::-1][:len(country_data)],
        text=[f"{r:.2f}%" for r in country_data["rate"]],
        textposition="auto"
    ))
    
    fig.update_layout(
        title="Chargeback Rate by Country (Top 10)",
        template="plotly_dark",
        height=350,
        xaxis_title="Chargeback Rate (%)",
        yaxis_title=""
    )
    
    return fig


def create_payment_method_chart(chargebacks: pd.DataFrame) -> go.Figure:
    """Create chargeback by payment method pie chart."""
    
    payment_data = chargebacks.groupby("payment_method")["amount"].sum().reset_index()
    
    fig = go.Figure(data=[go.Pie(
        labels=payment_data["payment_method"],
        values=payment_data["amount"],
        hole=0.4,
        marker_colors=["#6366f1", "#8b5cf6", "#a855f7", "#d946ef"]
    )])
    
    fig.update_layout(
        title="Chargeback Amount by Payment Method",
        template="plotly_dark",
        height=300
    )
    
    return fig


def main():
    """Main dashboard."""
    
    st.title("💳 Chargeback Analytics Dashboard")
    st.markdown("Monitor payment fraud, chargebacks, and dispute performance")
    
    # Load data
    try:
        transactions, chargebacks = load_data()
    except FileNotFoundError:
        st.error("Data files not found. Run `python data/generate_chargebacks.py` first.")
        return
    
    # Sidebar filters
    st.sidebar.header("Filters")
    
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(
            chargebacks["chargeback_date"].min().date(),
            chargebacks["chargeback_date"].max().date()
        )
    )
    
    if len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = chargebacks["chargeback_date"].min().date()
        end_date = chargebacks["chargeback_date"].max().date()
    
    # Calculate metrics
    metrics = calculate_metrics(transactions, chargebacks, start_date, end_date)
    
    # KPI Cards
    st.markdown("### Key Metrics")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "Total Transactions",
            f"{metrics['total_transactions']:,}",
        )
    
    with col2:
        st.metric(
            "Total Revenue",
            f"${metrics['total_revenue']:,.0f}",
        )
    
    with col3:
        st.metric(
            "Chargebacks",
            f"{metrics['total_chargebacks']:,}",
        )
    
    with col4:
        rate_color = "🔴" if metrics['chargeback_rate'] > 1 else "🟢"
        st.metric(
            f"Chargeback Rate {rate_color}",
            f"{metrics['chargeback_rate']:.2f}%",
        )
    
    with col5:
        st.metric(
            "Net Loss",
            f"${metrics['net_loss']:,.0f}",
        )
    
    # Second row of KPIs
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Disputes Filed", f"{int(metrics['disputes_filed']):,}")
    
    with col2:
        st.metric("Disputes Won", f"{int(metrics['disputes_won']):,}")
    
    with col3:
        st.metric("Win Rate", f"{metrics['dispute_win_rate']:.1f}%")
    
    with col4:
        st.metric("Recovered", f"${metrics['recovered']:,.0f}")
    
    st.markdown("---")
    
    # Charts
    st.markdown("### Trends")
    st.plotly_chart(create_trend_chart(chargebacks, transactions), use_container_width=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.plotly_chart(create_category_chart(chargebacks), use_container_width=True)
    
    with col2:
        st.plotly_chart(create_payment_method_chart(chargebacks), use_container_width=True)
    
    st.plotly_chart(create_country_chart(chargebacks, transactions), use_container_width=True)
    
    # Data tables
    st.markdown("---")
    st.markdown("### Recent Chargebacks")
    
    recent = chargebacks.sort_values("chargeback_date", ascending=False).head(20)[[
        "chargeback_id", "chargeback_date", "amount", "reason_category", 
        "reason_description", "country", "payment_method", "status"
    ]]
    
    st.dataframe(recent, use_container_width=True)


if __name__ == "__main__":
    main()
