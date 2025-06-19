import os
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

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


# Load conversation-region data joined with tweets for time filtering
def load_conversation_data(engine):
    query = """
    SELECT 
        c.conversation_id,
        c.airline,
        c.user_id AS conversation_user_id,
        urm.region,
        t.created_at
    FROM conversations c
    JOIN user_region_map urm ON c.user_id = urm.user_id
    JOIN tweets t ON c.conversation_id = t.in_reply_to_status_id
    WHERE LOWER(c.airline) = 'lufthansa'
    """
    return pd.read_sql(query, engine)


# Load data
df = load_conversation_data(engine)

# Parse created_at to datetime
df['parsed_date'] = pd.to_datetime(df['created_at'], errors='coerce')

# Apply filtering
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

# Clean and count
df['region'] = df['region'].replace('Netherlands', 'Europe')
df = df.groupby('region', as_index=False)['conversation_id'].nunique()
df = df.rename(columns={'conversation_id': 'conversation_count'})
df = df.sort_values(by='conversation_count', ascending=False)

# Highlight Germany
colors = ['#f5a623' if region == 'Germany' else '#4a90e2' for region in df['region']]

# Plot
plt.figure(figsize=(12, 6))
bars = plt.bar(df['region'], df['conversation_count'], color=colors)

# Add labels
for bar in bars:
    height = bar.get_height()
    plt.annotate(f'{height:,.0f}',
                 xy=(bar.get_x() + bar.get_width() / 2, height),
                 xytext=(0, 3),
                 textcoords="offset points",
                 ha='center', va='bottom')

plt.xlabel('Region')
plt.ylabel('Number of Conversations')
plt.title("Number of Lufthansa Conversations By Region", fontsize=20, fontweight='bold')

# Underline title
plt.gca().annotate('', 
                   xy=(0.2, 1.02), xycoords='axes fraction',
                   xytext=(0.8, 1.02), textcoords='axes fraction',
                   arrowprops=dict(arrowstyle='-', lw=2, color='black'))

plt.xticks(rotation=45)
plt.tight_layout()
plt.grid(axis='y', linestyle='--', alpha=0.6)
plt.show()
