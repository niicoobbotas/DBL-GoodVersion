import os
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from collections import defaultdict

airline_usernames = {
    "klm", "airfrance", "british_airways", "americanair", "lufthansa",
    "airberlin", "easyjet", "ryanair", "singaporeair",
    "qantas", "etihadairways", "virginatlantic"
}

PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "DblCork2025")
PG_HOST     = os.getenv("PG_HOST", "dbl.cjiyqqck0poz.eu-west-1.rds.amazonaws.com")
PG_PORT     = os.getenv("PG_PORT", "5432")
PG_DB       = os.getenv("PG_DB", "DBL")

engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}")

query = """
SELECT 
    LOWER(airline) as airline,
    COUNT(*) as mention_count
FROM conversations
WHERE airline IS NOT NULL
GROUP BY LOWER(airline)
ORDER BY mention_count DESC;
"""

df = pd.read_sql(query, engine)

plt.figure(figsize=(10, 6))
colors = ['orange' if name == 'lufthansa' else '#4169e1' for name in df['airline']]
plt.bar(df['airline'], df['mention_count'], color=colors, edgecolor='black')

plt.title("Number of conversations per airline", fontweight='bold')
plt.xlabel("Airline")
plt.ylabel("Number of Conversations")
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--', alpha=0.2)
plt.tight_layout()

# Save in the same directory as the Python file
output_path = os.path.join(os.path.dirname(__file__), "conversations_per_airline.png")
plt.savefig(output_path)
plt.show()
