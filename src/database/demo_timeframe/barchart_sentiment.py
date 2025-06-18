import os
from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt

#date In string format: 'YYYY-MM-DD'
# ------------------ PARAMETERS TO EDIT ------------------          
exact_date = None
start_date = None
end_date = None
month = None
year = None
week_of_date = None 
# --------------------------------------------------------

# Database connection parameters
PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "DblCork2025")
PG_HOST     = os.getenv("PG_HOST", "dbl.cjiyqqck0poz.eu-west-1.rds.amazonaws.com")
PG_PORT     = os.getenv("PG_PORT", "5432")
PG_DB       = os.getenv("PG_DB", "DBL")

# Create database engine
engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}")

print("Database connection established. Ready to execute queries.") 

# Load tweets function
def load_tweets(engine, exact_date=None, start_date=None, end_date=None, month=None, year=None, week_of_date=None):
    conditions = []

    date_column = "TO_TIMESTAMP(t.created_at, 'YYYY-MM-DD HH24:MI:SS')"  # adjust format if needed

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
        t.*, 
        c.sentiment_start_score, 
        c.sentiment_end_score,
        (c.sentiment_start_score + c.sentiment_end_score)/2 AS sentiment_score,
        c.sentiment_start
    FROM tweets t
    JOIN conversations c ON c.tweet_id = t.tweet_id
    WHERE {where_clause}
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
    print("Query params:", params)

    return pd.read_sql(query, engine, params=params)

# Lufthansa user ID
lufthansa_id = 124476322

# Load data filtered by Lufthansa user ID and non-null sentiment
df = load_tweets(
    engine,
    exact_date=exact_date,
    start_date=start_date,
    end_date=end_date,
    month=2,
    year=year,
    week_of_date=week_of_date
)

df = df[(df['user_id'] == lufthansa_id) & (df['in_reply_to_status_id'].notnull()) & (df['sentiment_start'].notnull())]

# Count sentiment labels
sentiment_counts = df["sentiment_start"].value_counts()

# Plot bar chart
plt.figure(figsize=(6, 4))
sentiment_counts.plot(kind="bar", color=["green", "gray", "red"])
plt.title("Lufthansa Sentiment Distribution")
plt.xlabel("Sentiment")
plt.ylabel("Number of Tweets")
plt.xticks(rotation=0)
plt.tight_layout()
plt.show()
