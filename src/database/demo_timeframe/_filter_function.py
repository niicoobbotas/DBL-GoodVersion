import os
from sqlalchemy import create_engine
import pandas as pd

PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "DblCork2025")
PG_HOST     = os.getenv("PG_HOST", "dbl.cjiyqqck0poz.eu-west-1.rds.amazonaws.com")
PG_PORT     = os.getenv("PG_PORT", "5432")
PG_DB       = os.getenv("PG_DB", "DBL")

engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}")

print("Database connection established. Ready to execute queries.")

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
        (c.sentiment_start_score + c.sentiment_end_score)/2 AS sentiment_score
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

    return pd.read_sql(query, engine, params=params)

# Example call
try:
    df = load_tweets(engine, month=2)
    print(df.head())
except Exception as e:
    print("Error:", e)

