import os
from sqlalchemy import create_engine
import pandas as pd
import plotly.express as px
from datetime import datetime

# Database connection parameters
PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "DblCork2025")
PG_HOST     = os.getenv("PG_HOST", "dbl.cjiyqqck0poz.eu-west-1.rds.amazonaws.com")
PG_PORT     = os.getenv("PG_PORT", "5432")
PG_DB       = os.getenv("PG_DB", "DBL")

# Create database engine
engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}")

# Query to get tweet counts by date
query = """
SELECT 
    DATE(created_at) as tweet_date,
    COUNT(*) as tweet_count
FROM tweets
GROUP BY DATE(created_at)
ORDER BY tweet_date;
"""

# Execute query and create DataFrame
with engine.connect() as conn:
    df = pd.read_sql(query, conn)

# Create interactive visualization using plotly
fig = px.line(df, 
              x='tweet_date', 
              y='tweet_count',
              title='Number of Tweets Over Time',
              labels={'tweet_date': 'Date', 'tweet_count': 'Number of Tweets'})

# Update layout for better readability
fig.update_layout(
    xaxis_title="Date",
    yaxis_title="Number of Tweets",
    hovermode='x unified',
    showlegend=False
)

# Show the plot
fig.show()

# Optionally save the plot as HTML
fig.write_html("tweet_timeline.html")
