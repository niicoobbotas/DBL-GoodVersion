import os
from sqlalchemy import create_engine
import pandas as pd
import plotly.graph_objects as go
import numpy as np

# List of airline user IDs
airline_ids = {
    56377143, 106062176, 18332190, 22536055, 124476322, 26223583,
    2182373406, 38676903, 1542862735, 253340062, 218730857, 45621423, 20626359
}

# Database connection parameters
PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "DblCork2025")
PG_HOST     = os.getenv("PG_HOST", "dbl.cjiyqqck0poz.eu-west-1.rds.amazonaws.com")
PG_PORT     = os.getenv("PG_PORT", "5432")
PG_DB       = os.getenv("PG_DB", "DBL")

# Create database engine
engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}")

# Convert airline_ids to a tuple for SQL
airline_ids_tuple = tuple(airline_ids)

# Query to get conversation stages and sentiments
query = f"""
WITH user_tweets AS (
    SELECT 
        t.in_reply_to_status_id as conversation_id,
        t.created_at,
        c.sentiment
    FROM tweets t
    JOIN conversations c ON t.in_reply_to_status_id = c.conversation_id
    WHERE t.in_reply_to_status_id IS NOT NULL
      AND t.user_id NOT IN {airline_ids_tuple}
      AND c.sentiment IS NOT NULL
),
conversation_stages AS (
    SELECT 
        conversation_id,
        -- first tweet
        FIRST_VALUE(sentiment) OVER (
            PARTITION BY conversation_id 
            ORDER BY created_at
        ) as start_sentiment,
        -- middle tweet
        CASE 
            WHEN COUNT(*) OVER (PARTITION BY conversation_id) = 2 THEN
                LAST_VALUE(sentiment) OVER (
                    PARTITION BY conversation_id 
                    ORDER BY created_at
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                )
            ELSE
                NTH_VALUE(sentiment, 2) OVER (
                    PARTITION BY conversation_id 
                    ORDER BY created_at
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                )
        END as middle_sentiment,
        -- last tweet
        LAST_VALUE(sentiment) OVER (
            PARTITION BY conversation_id 
            ORDER BY created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as end_sentiment
    FROM user_tweets
)
SELECT DISTINCT
    conversation_id,
    start_sentiment,
    middle_sentiment,
    end_sentiment
FROM conversation_stages
ORDER BY conversation_id;
"""

# Execute query and create DataFrame
with engine.connect() as conn:
    df = pd.read_sql(query, conn)

# Create nodes for each stage and sentiment
stages = ['Start', 'Middle', 'End']

# Define sentiment order (positive at top, neutral middle, negative bottom)
sentiment_order = ['positive', 'neutral', 'negative']
sentiments = [s for s in sentiment_order if s in df['start_sentiment'].unique()]

# Calculate percentages for each stage
total_conversations = len(df)
stage_percentages = {
    'Start': df['start_sentiment'].value_counts(normalize=True) * 100,
    'Middle': df['middle_sentiment'].value_counts(normalize=True) * 100,
    'End': df['end_sentiment'].value_counts(normalize=True) * 100
}

# Create node labels and indices only for combinations that exist in the data
node_labels = []
node_indices = {}
for stage, col in zip(stages, ['start_sentiment', 'middle_sentiment', 'end_sentiment']):
    for sentiment in df[col].unique():
        percentage = stage_percentages[stage].get(sentiment, 0)
        label = f"{stage} - {sentiment} ({percentage:.1f}%)"
        node_indices[(stage, sentiment)] = len(node_labels)
        node_labels.append(label)


sources = []
targets = []
values = []


sentiment_colors = {
    'positive': '#2ca02c', 
    'neutral': '#ff7f0e',   
    'negative': '#d62728'   
}
sentiment_link_colors = {
    'positive': 'rgba(44, 160, 44, 0.08)',   
    'neutral': 'rgba(255, 127, 14, 0.08)',  
    'negative': 'rgba(214, 39, 40, 0.08)'   
}


for _, row in df.iterrows():
    source_idx = node_indices[('Start', row['start_sentiment'])]
    target_idx = node_indices[('Middle', row['middle_sentiment'])]
    sources.append(source_idx)
    targets.append(target_idx)
    values.append(1)
    
    source_idx = node_indices[('Middle', row['middle_sentiment'])]
    target_idx = node_indices[('End', row['end_sentiment'])]
    sources.append(source_idx)
    targets.append(target_idx)
    values.append(1)

# Create Sankey diagram
fig = go.Figure(data=[go.Sankey(
    node=dict(
        pad=15,
        thickness=20,
        line=dict(color="black", width=0.5),
        label=node_labels,
        color=[sentiment_colors[label.split(' - ')[1].split(' (')[0]] for label in node_labels]
    ),
    link=dict(
        source=sources,
        target=targets,
        value=values,
        color=[sentiment_link_colors[node_labels[source].split(' - ')[1].split(' (')[0]] for source in sources]
    )
)])

fig.update_layout(
    title_text="User Tweet Sentiment Flow Across Conversation Stages",
    font_size=12,
    height=800
)

fig.show()

fig.write_html("sentiment_sankey.html") 