import os
from sqlalchemy import create_engine
import pandas as pd
from scipy import stats
import matplotlib.pyplot as plt
from scipy.stats import ttest_ind, sem
import numpy as np



PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "DblCork2025")
PG_HOST     = os.getenv("PG_HOST", "dbl.cjiyqqck0poz.eu-west-1.rds.amazonaws.com")
PG_PORT     = os.getenv("PG_PORT", "5432")
PG_DB       = os.getenv("PG_DB", "DBL")

engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}")


# 2) PULL CONVERSATIONS + REGION FROM user_region_map
sql = """
SELECT
  c.conversation_id,
  c.airline,
  c.sentiment_start_score,
  c.sentiment_end_score,
  urm.region
FROM conversations AS c
JOIN user_region_map AS urm
  ON c.user_id = urm.user_id
WHERE c.airline IN ('Lufthansa','KLM')
  AND urm.region  IN ('Germany','North America','Netherlands')
"""
df = pd.read_sql(sql, engine)

# 3) COMPUTE DELTA & DEDUPE
df['delta'] = df.sentiment_end_score - df.sentiment_start_score
df = df.drop_duplicates(subset='conversation_id', keep='first')

# 4) EXTRACT YOUR THREE GROUPS
lut_de = df[(df.airline=='Lufthansa') & (df.region=='Germany')]['delta']
lut_na = df[(df.airline=='Lufthansa') & (df.region=='North America')]['delta']
klm_nl = df[(df.airline=='KLM')        & (df.region=='Netherlands')]['delta']

print(f"Sample sizes → LH-DE: {len(lut_de)}, LH-NA: {len(lut_na)}, KLM-NL: {len(klm_nl)}\n")

# 5) RUN WELCH’S T-TESTS
t1, p1 = ttest_ind(lut_de, lut_na, equal_var=False)
t2, p2 = ttest_ind(lut_de, klm_nl, equal_var=False)

print(f"Lufthansa DE vs North America → t = {t1:.3f},  p = {p1:.3f}")
print(f"Lufthansa DE vs KLM Netherlands → t = {t2:.3f},  p = {p2:.3f}")

# 6) BOXPLOT


# Assume lut_de, lut_na, klm_nl are already defined as before.



# 1) Lufthansa Germany vs Lufthansa North America
fig, ax = plt.subplots(figsize=(6,5))
ax.boxplot([lut_de, lut_na],
           labels=["LH Germany","LH North America"],
           showfliers=False)
ax.set_ylabel("Sentiment change (end − start)")
ax.set_title("Lufthansa: Germany vs North America")
ax.grid(axis="y", linestyle="--", alpha=0.4)
plt.tight_layout()
plt.show()

# 2) Lufthansa Germany vs KLM Netherlands
fig, ax = plt.subplots(figsize=(6,5))
ax.boxplot([lut_de, klm_nl],
           labels=["LH Germany","KLM Netherlands"],
           showfliers=False)
ax.set_ylabel("Sentiment change (end − start)")
ax.set_title("Lufthansa Germany vs KLM Netherlands")
ax.grid(axis="y", linestyle="--", alpha=0.4)
plt.tight_layout()
plt.show()



