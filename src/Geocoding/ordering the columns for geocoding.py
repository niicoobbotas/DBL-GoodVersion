import pandas as pd

# Load your CSV (already UTF-8)
df = pd.read_csv(r'C:\Users\Admin\Documents\subjects\DBL project\data DBL\geocoded_with_region_UTF-8.csv')

df = df[df['normalized_location'].notnull() & (df['normalized_location'].str.strip() != '')]

# Reorder the columns
desired_order = ['normalized_location', 'latitude', 'longitude', 'confidence', 'region']
df = df[desired_order]

# Save to a new file
df.to_csv(r'C:\Users\Admin\Documents\subjects\DBL project\data DBL\geocoded_ordered_no_nulls_UTF-8.csv',
           index=False, 
           encoding='utf-8'
           )
