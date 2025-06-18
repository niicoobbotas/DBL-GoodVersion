import os
from sqlalchemy import create_engine
import pandas as pd
import plotly.graph_objects as go

# Lufthansa user ID
lufthansa_id = 124476322

# Database connection parameters
PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "DblCork2025")
PG_HOST     = os.getenv("PG_HOST", "dbl.cjiyqqck0poz.eu-west-1.rds.amazonaws.com")
PG_PORT     = os.getenv("PG_PORT", "5432")
PG_DB       = os.getenv("PG_DB", "DBL")

engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}")

# Query
query = f"""
WITH conversation_tweets AS (
    SELECT 
        t.in_reply_to_status_id as conversation_id,
        t.created_at,
        c.sentiment_start,
        c.sentiment_end
    FROM tweets t
    JOIN conversations c ON t.in_reply_to_status_id = c.conversation_id
    WHERE t.in_reply_to_status_id IS NOT NULL
      AND t.user_id = {lufthansa_id}
      AND c.sentiment_start IS NOT NULL
      AND c.sentiment_end IS NOT NULL
),
first_last_tweets AS (
    SELECT DISTINCT
        conversation_id,
        FIRST_VALUE(sentiment_start) OVER (
            PARTITION BY conversation_id 
            ORDER BY created_at
        ) as sentiment_start,
        LAST_VALUE(sentiment_end) OVER (
            PARTITION BY conversation_id 
            ORDER BY created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as sentiment_end
    FROM conversation_tweets
)
SELECT DISTINCT
    conversation_id,
    sentiment_start,
    sentiment_end
FROM first_last_tweets
ORDER BY conversation_id;
"""

# Query and create df
with engine.connect() as conn:
    df = pd.read_sql(query, conn)

sentiment_colors = {
    'positive': '#0072B2',  
    'neutral': '#E69F00',   
    'negative': '#D55E00'   
}

sentiments = ['positive', 'neutral', 'negative']
stages = ['Start', 'End']

# Calculate percentages
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