import pandas as pd
import matplotlib.pyplot as plt
import psycopg2

# Connect to your PostgreSQL database
conn = psycopg2.connect(
    host="dbl.cjiyqqck0poz.eu-west-1.rds.amazonaws.com",
    port=5432,
    dbname="DBL",
    user="postgres",
    password="DblCork2025",
    sslmode="require"
)

# SQL query to get Lufthansa conversation count per region
query = """
SELECT urm.region,
       COUNT(DISTINCT c.conversation_id) AS conversation_count
FROM conversations c
JOIN user_region_map urm ON c.user_id = urm.user_id
WHERE LOWER(c.airline) = 'lufthansa'
GROUP BY urm.region
ORDER BY conversation_count DESC;
"""

df = pd.read_sql_query(query, conn)
conn.close()

df['region'] = df['region'].replace('Netherlands', 'Europe')
df = df.groupby('region', as_index=False)['conversation_count'].sum()
df = df.sort_values(by='conversation_count', ascending=False)

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
# Bold title (no underline yet)
plt.title("Number of Lufthansa Conversations By Region", fontsize=20, fontweight='bold')

# Manual underline line under the title
plt.gca().annotate('', 
                   xy=(0.2, 1.02), xycoords='axes fraction',
                   xytext=(0.8, 1.02), textcoords='axes fraction',
                   arrowprops=dict(arrowstyle='-', lw=2, color='black'))

plt.xticks(rotation=45)
plt.tight_layout()
plt.grid(axis='y', linestyle='--', alpha=0.6)

plt.show()
