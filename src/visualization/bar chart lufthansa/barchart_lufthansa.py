import os
from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt

# Lufthansa user ID
lufthansa_id = 124476322

# Database connection parameters
PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "DblCork2025")
PG_HOST     = os.getenv("PG_HOST", "dbl.cjiyqqck0poz.eu-west-1.rds.amazonaws.com")
PG_PORT     = os.getenv("PG_PORT", "5432")
PG_DB       = os.getenv("PG_DB", "DBL")

# Create database engine
engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}")

# Query: All Lufthansa tweets with sentiment
query = f"""
SELECT 
    t.id,
    t.created_at,
    c.sentiment
FROM tweets t
JOIN conversations c ON t.in_reply_to_status_id = c.conversation_id
WHERE t.in_reply_to_status_id IS NOT NULL
  AND t.user_id = {lufthansa_id}
  AND c.sentiment IS NOT NULL
ORDER BY t.created_at;
"""

# Run the query and load into DataFrame
df = pd.read_sql(query, engine)

# Count sentiment labels
sentiment_counts = df["sentiment"].value_counts()

# Plot bar chart
plt.figure(figsize=(6, 4))
sentiment_counts.plot(kind="bar", color=["green", "gray", "red"])
plt.title("Lufthansa Sentiment Distribution")
plt.xlabel("Sentiment")
plt.ylabel("Number of Tweets")
plt.xticks(rotation=0)
plt.tight_layout()
plt.show()
