import os
import pandas as pd
from sqlalchemy import create_engine
import plotly.graph_objects as go

#date In string format: 'YYYY-MM-DD'
# ------------------ PARAMETERS TO EDIT ------------------
exact_date = None           
start_date = None           
end_date = None             
month = None               
year = None                 
week_of_date = None         
# --------------------------------------------------------

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

engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}")

print("Database connection established. Ready to execute queries.")

# Load tweets function (unfiltered SQL)
def load_tweets(engine):
    query = """
    SELECT 
        t.*, 
        c.airline,
        c.sentiment_start,
        c.sentiment_end
    FROM tweets t
    JOIN conversations c ON c.tweet_id = t.tweet_id
    WHERE c.airline IS NOT NULL
    """
    return pd.read_sql(query, engine)

# Load data
df = load_tweets(engine)

# Parse created_at as datetime
df['parsed_date'] = pd.to_datetime(df['created_at'], errors='coerce')

# Apply date filtering in pandas
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

# Plotting Sankey diagram
sentiment_colors = {
    'positive': '#0072B2',
    'neutral': '#E69F00',
    'negative': '#D55E00'
}

start_percentages = df['sentiment_start'].value_counts(normalize=True) * 100
end_percentages = df['sentiment_end'].value_counts(normalize=True) * 100

node_labels = []
node_indices = {}

for stage in ['Start', 'End']:
    col = 'sentiment_start' if stage == 'Start' else 'sentiment_end'
    percentages = start_percentages if stage == 'Start' else end_percentages
    for sentiment in ['positive', 'neutral', 'negative']:
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
        color=node_colors,
        y=[0.1 if 'positive' in label else 0.5 if 'neutral' in label else 0.9 for label in node_labels]
    ),
    link=dict(
        source=sources,
        target=targets,
        value=values,
        color=link_colors
    )
)])

fig.update_layout(
    title_text="User Tweet Sentiment Flow",
    font_size=12,
    height=800,
    margin=dict(t=50, b=50, l=50, r=50)
)

fig.show()
