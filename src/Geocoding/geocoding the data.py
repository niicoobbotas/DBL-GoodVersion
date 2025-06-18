import pandas as pd
import os
import time
from geopy.geocoders import Nominatim

# --- Setup ---
geolocator = Nominatim(user_agent="geo_cluster")

def geocode_location(loc):
    try:
        location = geolocator.geocode(loc, timeout=10)
        if location:
            return {
                "latitude": location.latitude,
                "longitude": location.longitude,
                "confidence": "high"
            }
    except Exception:
        pass
    return {
        "latitude": None,
        "longitude": None,
        "confidence": "low"
    }

# --- Paths ---
input_path = r'C:\Users\Admin\Documents\subjects\DBL project\data DBL\unique_locations.csv'
output_path = r'C:\Users\Admin\Documents\subjects\DBL project\data DBL\geocoded_locations.csv'

# --- Load input ---
df_all = pd.read_csv(
    input_path,
    names=["normalized_location"],
    header=None,
    quotechar='"',
    sep=',',
    on_bad_lines='skip',
    engine='python',
    encoding='utf-8'
)

# --- Load or create output ---
if os.path.exists(output_path):
    df_done = pd.read_csv(output_path)
    done_set = set(df_done['normalized_location'].dropna().unique())
    print(f" Resuming from existing file. {len(done_set)} locations already geocoded.")
else:
    df_done = pd.DataFrame(columns=["normalized_location", "latitude", "longitude", "confidence"])
    done_set = set()
    print(" Starting fresh geocoding.")

# --- Filter only new entries ---
df_todo = df_all[~df_all['normalized_location'].isin(done_set)].reset_index(drop=True)
print(f"🔍 Locations left to geocode: {len(df_todo)}")

# --- Geocode loop ---
for i, row in df_todo.iterrows():
    loc = row['normalized_location']
    result = geocode_location(loc)
    
    # Append result
    df_done = pd.concat([
        df_done,
        pd.DataFrame([{
            "normalized_location": loc,
            **result
        }])
    ], ignore_index=True)
    
    # Log + Save every 100
    if (i + 1) % 100 == 0 or (i + 1) == len(df_todo):
        df_done.to_csv(output_path, index=False)
        print(f"{i + 1} / {len(df_todo)} done — saved.")

    time.sleep(1)  # Respect Nominatim's rate limit

print(" All done. Final file saved at:")
print(output_path)
