import os
from sqlalchemy import create_engine
import pandas as pd

# Database connection parameters
PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "DblCork2025")
PG_HOST     = os.getenv("PG_HOST", "dbl.cjiyqqck0poz.eu-west-1.rds.amazonaws.com")
PG_PORT     = os.getenv("PG_PORT", "5432")
PG_DB       = os.getenv("PG_DB", "DBL")

# Create database engine
engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}")

# Query to get all tables
query = """
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public';
"""

# Execute query
with engine.connect() as conn:
    df_tables = pd.read_sql(query, conn)
print("Tables in database:")
print(df_tables)

# For each table, get its columns
for table in df_tables['table_name']:
    query = f"""
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns 
    WHERE table_name = '{table}'
    ORDER BY ordinal_position;
    """
    with engine.connect() as conn:
        df_columns = pd.read_sql(query, conn)
    print(f"\nColumns in {table} table:")
    print(df_columns.to_string()) 