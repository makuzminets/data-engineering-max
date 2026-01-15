"""
Generate all visualizations from the email funnel analysis.
Saves charts as PNG files in the assets folder.
"""

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy import stats
import os

# Create assets folder if not exists
os.makedirs('assets', exist_ok=True)

# Load datasets
print("Loading data...")
users = pd.read_csv('data/users.csv')
events = pd.read_csv('data/email_events.csv')
conversions = pd.read_csv('data/conversions.csv')

# Convert date columns
users['signup_date'] = pd.to_datetime(users['signup_date'])
events['event_time'] = pd.to_datetime(events['event_time'])
conversions['event_time'] = pd.to_datetime(conversions['event_time'])

print(f"Users: {len(users):,}")
print(f"Email Events: {len(events):,}")
print(f"Conversions: {len(conversions):,}")

# ============================================
# 1. FUNNEL ANALYSIS
# ============================================
print("\nGenerating funnel chart...")

total_users = len(users)
emails_sent = events[events['event_type'] == 'welcome_email_sent']['user_id'].nunique()
emails_opened = events[(events['event_type'] == 'email_opened') & (events['email_type'] == 'welcome')]['user_id'].nunique()
emails_clicked = events[(events['event_type'] == 'email_clicked') & (events['email_type'] == 'welcome')]['user_id'].nunique()
trials_started = conversions[conversions['event_type'] == 'trial_started']['user_id'].nunique()
paid_users = conversions[conversions['event_type'] == 'paid_conversion']['user_id'].nunique()

funnel_data = pd.DataFrame({
    'Stage': ['1. Signup', '2. Email Sent', '3. Email Opened', '4. Link Clicked', '5. Trial Started', '6. Paid Conversion'],
    'Users': [total_users, emails_sent, emails_opened, emails_clicked, trials_started, paid_users]
})

funnel_data['Conversion Rate (from Signup)'] = (funnel_data['Users'] / total_users * 100).round(2)
funnel_data['Stage Conversion Rate'] = (funnel_data['Users'] / funnel_data['Users'].shift(1) * 100).round(2)
funnel_data['Drop-off'] = (funnel_data['Users'].shift(1) - funnel_data['Users']).fillna(0).astype(int)
funnel_data['Drop-off Rate'] = (funnel_data['Drop-off'] / funnel_data['Users'].shift(1) * 100).round(2)

# Funnel visualization
fig = go.Figure(go.Funnel(
    y=funnel_data['Stage'],
    x=funnel_data['Users'],
    textinfo="value+percent initial",
    textposition="inside",
    marker={"color": ["#3366CC", "#4285F4", "#5C9EFF", "#85B8FF", "#ADD1FF", "#34A853"]},
    connector={"line": {"color": "royalblue", "dash": "solid", "width": 2}}
))

fig.update_layout(
    title="Email Marketing Funnel",
    title_x=0.5,
    height=500,
    font=dict(size=14),
    paper_bgcolor='white',
    plot_bgcolor='white'
)

fig.write_image("assets/01_funnel_chart.png", scale=2)
print("  Saved: assets/01_funnel_chart.png")

# ============================================
# 2. DROP-OFF ANALYSIS
# ============================================
print("\nGenerating drop-off chart...")

dropoff_data = funnel_data[funnel_data['Drop-off'] > 0].copy()
dropoff_data['Stage Transition'] = [
    'Signup → Email Sent',
    'Email Sent → Opened',
    'Opened → Clicked',
    'Clicked → Trial',
    'Trial → Paid'
]

fig = px.bar(
    dropoff_data,
    x='Stage Transition',
    y='Drop-off',
    text='Drop-off Rate',
    color='Drop-off',
    color_continuous_scale='Reds'
)

fig.update_traces(texttemplate='%{text}%', textposition='outside')
fig.update_layout(
    title='Drop-off at Each Funnel Stage',
    title_x=0.5,
    xaxis_title='Funnel Transition',
    yaxis_title='Users Lost',
    height=450,
    showlegend=False,
    paper_bgcolor='white',
    plot_bgcolor='white'
)

fig.write_image("assets/02_dropoff_chart.png", scale=2)
print("  Saved: assets/02_dropoff_chart.png")

# ============================================
# 3. COHORT ANALYSIS
# ============================================
print("\nGenerating cohort analysis...")

# Create user stage mapping
paid_users_set = set(conversions[conversions['event_type'] == 'paid_conversion']['user_id'])
trial_users_set = set(conversions[conversions['event_type'] == 'trial_started']['user_id'])
clicked_users_set = set(events[(events['event_type'] == 'email_clicked') & (events['email_type'] == 'welcome')]['user_id'])
opened_users_set = set(events[(events['event_type'] == 'email_opened') & (events['email_type'] == 'welcome')]['user_id'])
sent_users_set = set(events[events['event_type'] == 'welcome_email_sent']['user_id'])

def get_user_stage_fast(user_id):
    if user_id in paid_users_set:
        return 'Paid'
    if user_id in trial_users_set:
        return 'Trial'
    if user_id in clicked_users_set:
        return 'Clicked'
    if user_id in opened_users_set:
        return 'Opened'
    if user_id in sent_users_set:
        return 'Email Sent'
    return 'Signup Only'

users['max_stage'] = users['user_id'].apply(get_user_stage_fast)

cohort_stats = users.groupby('signup_month').agg(
    total_users=('user_id', 'count'),
    opened=('max_stage', lambda x: (x.isin(['Opened', 'Clicked', 'Trial', 'Paid'])).sum()),
    clicked=('max_stage', lambda x: (x.isin(['Clicked', 'Trial', 'Paid'])).sum()),
    trial=('max_stage', lambda x: (x.isin(['Trial', 'Paid'])).sum()),
    paid=('max_stage', lambda x: (x == 'Paid').sum())
).reset_index()

cohort_stats['open_rate'] = (cohort_stats['opened'] / cohort_stats['total_users'] * 100).round(2)
cohort_stats['click_rate'] = (cohort_stats['clicked'] / cohort_stats['total_users'] * 100).round(2)
cohort_stats['trial_rate'] = (cohort_stats['trial'] / cohort_stats['total_users'] * 100).round(2)
cohort_stats['conversion_rate'] = (cohort_stats['paid'] / cohort_stats['total_users'] * 100).round(2)

# Cohort line chart
fig = go.Figure()

fig.add_trace(go.Scatter(
    x=cohort_stats['signup_month'],
    y=cohort_stats['open_rate'],
    mode='lines+markers',
    name='Open Rate',
    line=dict(color='#4285F4', width=2)
))

fig.add_trace(go.Scatter(
    x=cohort_stats['signup_month'],
    y=cohort_stats['click_rate'],
    mode='lines+markers',
    name='Click Rate',
    line=dict(color='#FBBC04', width=2)
))

fig.add_trace(go.Scatter(
    x=cohort_stats['signup_month'],
    y=cohort_stats['trial_rate'],
    mode='lines+markers',
    name='Trial Rate',
    line=dict(color='#EA4335', width=2)
))

fig.add_trace(go.Scatter(
    x=cohort_stats['signup_month'],
    y=cohort_stats['conversion_rate'],
    mode='lines+markers',
    name='Paid Conversion',
    line=dict(color='#34A853', width=2)
))

fig.update_layout(
    title='Funnel Metrics by Signup Cohort',
    title_x=0.5,
    xaxis_title='Signup Month',
    yaxis_title='Conversion Rate (%)',
    height=500,
    legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
    paper_bgcolor='white',
    plot_bgcolor='white'
)

fig.write_image("assets/03_cohort_trends.png", scale=2)
print("  Saved: assets/03_cohort_trends.png")

# Cohort heatmap
cohort_heatmap = cohort_stats[['signup_month', 'open_rate', 'click_rate', 'trial_rate', 'conversion_rate']].set_index('signup_month').T

fig = px.imshow(
    cohort_heatmap,
    text_auto='.1f',
    color_continuous_scale='Blues',
    aspect='auto'
)

fig.update_layout(
    title='Cohort Performance Heatmap (Conversion Rates %)',
    title_x=0.5,
    xaxis_title='Signup Month',
    yaxis_title='Metric',
    height=300,
    paper_bgcolor='white'
)

fig.write_image("assets/04_cohort_heatmap.png", scale=2)
print("  Saved: assets/04_cohort_heatmap.png")

# ============================================
# 4. A/B TEST ANALYSIS
# ============================================
print("\nGenerating A/B test results...")

ab_results = users.groupby('ab_variant').agg(
    total_users=('user_id', 'count'),
    opened=('max_stage', lambda x: (x.isin(['Opened', 'Clicked', 'Trial', 'Paid'])).sum()),
    clicked=('max_stage', lambda x: (x.isin(['Clicked', 'Trial', 'Paid'])).sum()),
    converted=('max_stage', lambda x: (x == 'Paid').sum())
).reset_index()

ab_results['open_rate'] = (ab_results['opened'] / ab_results['total_users'] * 100).round(2)
ab_results['click_rate'] = (ab_results['clicked'] / ab_results['total_users'] * 100).round(2)
ab_results['conversion_rate'] = (ab_results['converted'] / ab_results['total_users'] * 100).round(2)

# A/B test visualization
fig = make_subplots(
    rows=1, cols=3,
    subplot_titles=['Open Rate (%)', 'Click Rate (%)', 'Conversion Rate (%)']
)

colors = {'control': '#4285F4', 'variant_a': '#34A853', 'variant_b': '#EA4335'}

for i, metric in enumerate(['open_rate', 'click_rate', 'conversion_rate'], 1):
    fig.add_trace(
        go.Bar(
            x=ab_results['ab_variant'],
            y=ab_results[metric],
            marker_color=[colors[v] for v in ab_results['ab_variant']],
            text=ab_results[metric],
            textposition='outside',
            showlegend=False
        ),
        row=1, col=i
    )

fig.update_layout(
    title='A/B Test Results by Variant',
    title_x=0.5,
    height=400,
    paper_bgcolor='white',
    plot_bgcolor='white'
)

fig.write_image("assets/05_ab_test_results.png", scale=2)
print("  Saved: assets/05_ab_test_results.png")

# ============================================
# 5. SEGMENTATION BY SOURCE
# ============================================
print("\nGenerating source analysis...")

source_stats = users.groupby('source').agg(
    total_users=('user_id', 'count'),
    opened=('max_stage', lambda x: (x.isin(['Opened', 'Clicked', 'Trial', 'Paid'])).sum()),
    clicked=('max_stage', lambda x: (x.isin(['Clicked', 'Trial', 'Paid'])).sum()),
    converted=('max_stage', lambda x: (x == 'Paid').sum())
).reset_index()

source_stats['open_rate'] = (source_stats['opened'] / source_stats['total_users'] * 100).round(2)
source_stats['click_rate'] = (source_stats['clicked'] / source_stats['total_users'] * 100).round(2)
source_stats['conversion_rate'] = (source_stats['converted'] / source_stats['total_users'] * 100).round(2)
source_stats = source_stats.sort_values('conversion_rate', ascending=False)

fig = make_subplots(
    rows=1, cols=2,
    subplot_titles=['Users by Source', 'Conversion Rate by Source'],
    specs=[[{"type": "pie"}, {"type": "bar"}]]
)

fig.add_trace(
    go.Pie(
        labels=source_stats['source'],
        values=source_stats['total_users'],
        textinfo='percent+label',
        hole=0.3
    ),
    row=1, col=1
)

fig.add_trace(
    go.Bar(
        x=source_stats['source'],
        y=source_stats['conversion_rate'],
        marker_color='#4285F4',
        text=source_stats['conversion_rate'].apply(lambda x: f'{x}%'),
        textposition='outside'
    ),
    row=1, col=2
)

fig.update_layout(
    title='Traffic Source Analysis',
    title_x=0.5,
    height=450,
    showlegend=False,
    paper_bgcolor='white',
    plot_bgcolor='white'
)

fig.write_image("assets/06_source_analysis.png", scale=2)
print("  Saved: assets/06_source_analysis.png")

# ============================================
# 6. SEGMENTATION BY DEVICE
# ============================================
print("\nGenerating device analysis...")

device_stats = users.groupby('device').agg(
    total_users=('user_id', 'count'),
    opened=('max_stage', lambda x: (x.isin(['Opened', 'Clicked', 'Trial', 'Paid'])).sum()),
    clicked=('max_stage', lambda x: (x.isin(['Clicked', 'Trial', 'Paid'])).sum()),
    converted=('max_stage', lambda x: (x == 'Paid').sum())
).reset_index()

device_stats['open_rate'] = (device_stats['opened'] / device_stats['total_users'] * 100).round(2)
device_stats['click_rate'] = (device_stats['clicked'] / device_stats['total_users'] * 100).round(2)
device_stats['conversion_rate'] = (device_stats['converted'] / device_stats['total_users'] * 100).round(2)

fig = go.Figure()

metrics = ['open_rate', 'click_rate', 'conversion_rate']
colors_metrics = ['#4285F4', '#FBBC04', '#34A853']

for metric, color in zip(metrics, colors_metrics):
    fig.add_trace(go.Bar(
        name=metric.replace('_', ' ').title(),
        x=device_stats['device'],
        y=device_stats[metric],
        marker_color=color
    ))

fig.update_layout(
    title='Funnel Metrics by Device Type',
    title_x=0.5,
    xaxis_title='Device',
    yaxis_title='Rate (%)',
    barmode='group',
    height=400,
    paper_bgcolor='white',
    plot_bgcolor='white'
)

fig.write_image("assets/07_device_analysis.png", scale=2)
print("  Saved: assets/07_device_analysis.png")

# ============================================
# 7. SEGMENTATION BY COUNTRY
# ============================================
print("\nGenerating country analysis...")

country_stats = users.groupby('country').agg(
    total_users=('user_id', 'count'),
    converted=('max_stage', lambda x: (x == 'Paid').sum())
).reset_index()

country_stats['conversion_rate'] = (country_stats['converted'] / country_stats['total_users'] * 100).round(2)
country_stats = country_stats.sort_values('conversion_rate', ascending=False)

fig = px.bar(
    country_stats,
    x='country',
    y='conversion_rate',
    color='conversion_rate',
    color_continuous_scale='Greens',
    text='conversion_rate'
)

fig.update_traces(texttemplate='%{text}%', textposition='outside')
fig.update_layout(
    title='Paid Conversion Rate by Country',
    title_x=0.5,
    xaxis_title='Country',
    yaxis_title='Conversion Rate (%)',
    height=400,
    showlegend=False,
    paper_bgcolor='white',
    plot_bgcolor='white'
)

fig.write_image("assets/08_country_analysis.png", scale=2)
print("  Saved: assets/08_country_analysis.png")

# ============================================
# 8. REVENUE ANALYSIS
# ============================================
print("\nGenerating revenue analysis...")

paid_conversions = conversions[conversions['event_type'] == 'paid_conversion'].copy()

if 'revenue' not in paid_conversions.columns:
    plan_prices = {'basic': 29, 'pro': 79, 'enterprise': 199}
    paid_conversions['revenue'] = paid_conversions['plan'].map(plan_prices)

revenue_by_plan = paid_conversions.groupby('plan').agg(
    customers=('user_id', 'count'),
    total_revenue=('revenue', 'sum')
).reset_index()

fig = make_subplots(
    rows=1, cols=2,
    subplot_titles=['Customers by Plan', 'Revenue by Plan'],
    specs=[[{"type": "pie"}, {"type": "pie"}]]
)

fig.add_trace(
    go.Pie(
        labels=revenue_by_plan['plan'],
        values=revenue_by_plan['customers'],
        name='Customers',
        marker_colors=['#4285F4', '#34A853', '#FBBC04']
    ),
    row=1, col=1
)

fig.add_trace(
    go.Pie(
        labels=revenue_by_plan['plan'],
        values=revenue_by_plan['total_revenue'],
        name='Revenue',
        marker_colors=['#4285F4', '#34A853', '#FBBC04']
    ),
    row=1, col=2
)

fig.update_layout(
    title='Revenue Distribution by Plan',
    title_x=0.5,
    height=400,
    paper_bgcolor='white'
)

fig.write_image("assets/09_revenue_by_plan.png", scale=2)
print("  Saved: assets/09_revenue_by_plan.png")

# Revenue by source
paid_with_source = paid_conversions.merge(users[['user_id', 'source']], on='user_id')

revenue_by_source = paid_with_source.groupby('source').agg(
    customers=('user_id', 'count'),
    total_revenue=('revenue', 'sum')
).reset_index()

source_users = users.groupby('source')['user_id'].count().reset_index()
source_users.columns = ['source', 'total_signups']

revenue_by_source = revenue_by_source.merge(source_users, on='source')
revenue_by_source['revenue_per_signup'] = (revenue_by_source['total_revenue'] / revenue_by_source['total_signups']).round(2)
revenue_by_source = revenue_by_source.sort_values('revenue_per_signup', ascending=False)

fig = px.bar(
    revenue_by_source,
    x='source',
    y='revenue_per_signup',
    color='revenue_per_signup',
    color_continuous_scale='Greens',
    text='revenue_per_signup'
)

fig.update_traces(texttemplate='$%{text}', textposition='outside')
fig.update_layout(
    title='Revenue per Signup by Traffic Source',
    title_x=0.5,
    xaxis_title='Traffic Source',
    yaxis_title='Revenue per Signup ($)',
    height=400,
    showlegend=False,
    paper_bgcolor='white',
    plot_bgcolor='white'
)

fig.write_image("assets/10_revenue_by_source.png", scale=2)
print("  Saved: assets/10_revenue_by_source.png")

print("\n" + "="*50)
print("All visualizations generated successfully!")
print("="*50)
print(f"\nTotal charts: 10")
print(f"Location: assets/")
