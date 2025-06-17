import os
import pandas as pd
from sqlalchemy import create_engine
import plotly.graph_objects as go

# ------------------ PARAMETERS TO EDIT ------------------
exact_date = None
start_date = None
end_date = None
month = None
year = None
week_of_date = None
# --------------------------------------------------------

# Lufthansa user ID
lufthansa_id = 124476322

# DB connection setup
PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "DblCork2025")
PG_HOST     = os.getenv("PG_HOST", "dbl.cjiyqqck0poz.eu-west-1.rds.amazonaws.com")
PG_PORT     = os.getenv("PG_PORT", "5432")
PG_DB       = os.getenv("PG_DB", "DBL")

engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}")

# Function to load Lufthansa tweets
def load_tweets(engine, exact_date=None, start_date=None, end_date=None, month=None, year=None, week_of_date=None):
    conditions = []

    date_column = "TO_TIMESTAMP(t.created_at, 'YYYY-MM-DD HH24:MI:SS')"

    if exact_date:
        conditions.append(f"{date_column}::date = %(exact_date)s")
    if start_date and end_date:
        conditions.append(f"{date_column} >= %(start_date)s AND {date_column} < %(end_date)s")
    if month and year:
        conditions.append(f"EXTRACT(MONTH FROM {date_column}) = %(month)s")
        conditions.append(f"EXTRACT(YEAR FROM {date_column}) = %(year)s")
    if week_of_date:
        conditions.append(f"DATE_TRUNC('week', {date_column}) = DATE_TRUNC('week', %(week_of_date)s::date)")

    where_clause = " AND ".join(conditions) if conditions else "TRUE"

    query = f"""
    SELECT 
        c.conversation_id,
        c.sentiment_start,
        c.sentiment_end
    FROM conversations c
    WHERE c.conversation_id IN (
        SELECT DISTINCT t.in_reply_to_status_id 
        FROM tweets t
        WHERE t.user_id = {lufthansa_id}
        AND {where_clause}
    )
    AND c.sentiment_start IS NOT NULL
    AND c.sentiment_end IS NOT NULL
    ORDER BY c.conversation_id;
    """

    params = {
        "exact_date": exact_date,
        "start_date": start_date,
        "end_date": end_date,
        "month": month,
        "year": year,
        "week_of_date": week_of_date,
    }

    params = {k: v for k, v in params.items() if v is not None}

    return pd.read_sql(query, engine, params=params)

# Load Lufthansa data
df = load_tweets(
    engine,
    exact_date=exact_date,
    start_date=start_date,
    end_date=end_date,
    month=month,
    year=year,
    week_of_date=week_of_date
)

# Sankey chart logic
sentiment_colors = {
    'positive': '#0072B2',  
    'neutral': '#E69F00',   
    'negative': '#D55E00'   
}

sentiments = ['positive', 'neutral', 'negative']
stages = ['Start', 'End']

start_percentages = df['sentiment_start'].value_counts(normalize=True) * 100
end_percentages = df['sentiment_end'].value_counts(normalize=True) * 100

node_labels = []
node_indices = {}
for stage, col, percentages in zip(['Start', 'End'], ['sentiment_start', 'sentiment_end'], [start_percentages, end_percentages]):
    for sentiment in sentiments:
        if sentiment in df[col].unique():
            pct = percentages.get(sentiment, 0)
            label = f"{sentiment.capitalize()} ({pct:.1f}%)"
            node_indices[(stage, sentiment)] = len(node_labels)
            node_labels.append(label)

links_df = df.groupby(['sentiment_start', 'sentiment_end']).size().reset_index(name='value')

sources = []
targets = []
values = []
link_colors = []
for _, row in links_df.iterrows():
    s_idx = node_indices[('Start', row['sentiment_start'])]
    e_idx = node_indices[('End', row['sentiment_end'])]
    sources.append(s_idx)
    targets.append(e_idx)
    values.append(row['value'])
    color = sentiment_colors[row['sentiment_start']]
    link_colors.append(f'rgba({int(color[1:3],16)},{int(color[3:5],16)},{int(color[5:7],16)},0.5)')

node_colors = []
for label in node_labels:
    sentiment = label.split(' ')[0].lower()
    node_colors.append(sentiment_colors[sentiment])

fig = go.Figure(data=[go.Sankey(
    node=dict(
        pad=15,
        thickness=20,
        line=dict(color="black", width=0.5),
        label=node_labels,
        color=node_colors
    ),
    link=dict(
        source=sources,
        target=targets,
        value=values,
        color=link_colors
    )
)])

fig.update_layout(
    title_text="Lufthansa Tweet Sentiment Flow",
    font_size=12,
    height=800
)

fig.write_html("src/visualization/sankey diagram/sentiment_sankey_lufthansa.html")
fig.show()

