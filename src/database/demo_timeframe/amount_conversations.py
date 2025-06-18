import os
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import datetime

from sympy import N

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
def load_tweets(engine):
    query = """
    SELECT 
        t.*, 
        c.airline
    FROM tweets t
    JOIN conversations c ON c.tweet_id = t.tweet_id
    WHERE c.airline IS NOT NULL
    """
    return pd.read_sql(query, engine)

# Load data
df = load_tweets(engine)

# Parse timestamps
df['parsed_date'] = pd.to_datetime(df['created_at'], format='%a %b %d %H:%M:%S %z %Y', errors='coerce')

# Apply date-based filtering in Pandas
if exact_date:
    df = df[df['parsed_date'].dt.date == pd.to_datetime(exact_date).date()]
if start_date and end_date:
    df = df[(df['parsed_date'] >= pd.to_datetime(start_date)) & (df['parsed_date'] < pd.to_datetime(end_date))]
if week_of_date:
    week_start = pd.to_datetime(week_of_date) - pd.to_timedelta(pd.to_datetime(week_of_date).weekday(), unit='D')
    week_end = week_start + pd.Timedelta(days=7)
    df = df[(df['parsed_date'] >= week_start) & (df['parsed_date'] < week_end)]
if month and year:
    df = df[(df['parsed_date'].dt.month == month) & (df['parsed_date'].dt.year == year)]

print("Filtered date range:", df['parsed_date'].min(), "to", df['parsed_date'].max())

# Count conversations per airline
df['airline'] = df['airline'].str.lower()
mention_counts = df['airline'].value_counts().sort_values(ascending=False)

# Plot
plt.figure(figsize=(10, 6))
colors = ['orange' if name == 'lufthansa' else '#4169e1' for name in mention_counts.index]
plt.bar(mention_counts.index, mention_counts.values, color=colors, edgecolor='black')

plt.title("Number of conversations per airline", fontweight='bold')
plt.xlabel("Airline")
plt.ylabel("Number of Conversations")
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--', alpha=0.2)
plt.tight_layout()
plt.show()
