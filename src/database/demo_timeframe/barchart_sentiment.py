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

# Load tweets function (no filtering in SQL, all in pandas)
def load_tweets(engine):
    query = """
    SELECT 
        t.*, 
        c.sentiment_start_score, 
        c.sentiment_end_score,
        (c.sentiment_start_score + c.sentiment_end_score)/2 AS sentiment_score,
        c.sentiment_start,
        c.user_id,
        c.in_reply_to_status_id
    FROM tweets t
    JOIN conversations c ON c.tweet_id = t.tweet_id
    """
    return pd.read_sql(query, engine)

# Load all data
df = load_tweets(engine)

# Parse created_at as datetime
df['parsed_date'] = pd.to_datetime(df['created_at'], errors='coerce')

# Filter by parameters in pandas
if exact_date:
    df = df[df['parsed_date'].dt.date == pd.to_datetime(exact_date).date()]

if start_date and end_date:
    df = df[(df['parsed_date'] >= pd.to_datetime(start_date)) & (df['parsed_date'] < pd.to_datetime(end_date))]

if month and year:
    df = df[(df['parsed_date'].dt.month == month) & (df['parsed_date'].dt.year == year)]

if week_of_date:
    week_start = pd.to_datetime(week_of_date) - pd.to_timedelta(pd.to_datetime(week_of_date).weekday(), unit='d')
    week_end = week_start + pd.Timedelta(days=7)
    df = df[(df['parsed_date'] >= week_start) & (df['parsed_date'] < week_end)]

# Lufthansa user ID
lufthansa_id = 124476322

# Further filtering for Lufthansa user, reply tweets, and sentiment not null
df = df[(df['user_id'] == lufthansa_id) & 
        (df['in_reply_to_status_id'].notnull()) & 
        (df['sentiment_start'].notnull())]

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

