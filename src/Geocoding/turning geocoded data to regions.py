import pandas as pd
import reverse_geocoder as rg

# --- Load data ---
df = pd.read_csv(r'C:\Users\Admin\Documents\subjects\DBL project\data DBL\geocoded_locations.csv')

# --- Filter high-confidence results only ---
df = df[df['confidence'] == 'high'].copy()

# --- Get lat/lon as tuples ---
coords = list(zip(df['latitude'], df['longitude']))

# --- Reverse geocode to get country codes ---
results = rg.search(coords, mode=1)  # mode=1 = quiet mode

# --- Add country and region based on logic ---
df['country'] = [r['cc'] for r in results]

# --- Define regions ---
def map_region(country):
    if country == 'DE':
        return 'Germany'
    elif country == 'NL':
        return 'Netherlands'
    elif country in ['US', 'CA', 'MX', 'GT', 'HN', 'NI', 'PA']:
        return 'North America'
    elif country in ['BR', 'AR', 'CO', 'CL', 'PE', 'VE', 'UY', 'BO', 'PY', 'EC', 'GY', 'SR', 'GF']:
        return 'South America'
    elif country in [
        'GB', 'FR', 'ES', 'IT', 'PL', 'RO', 'SE', 'CH', 'BE', 'AT', 'DK', 'NO', 'FI', 'GR',
        'PT', 'IE', 'CZ', 'HU', 'SK', 'BG', 'HR', 'SI', 'LU', 'LI', 'MC', 'SM', 'VA', 'IS', 'EE', 'LV', 'LT', 'AL', 'MK'
    ]:
        return 'Europe'
    elif country in [
        'CN', 'IN', 'JP', 'KR', 'ID', 'TH', 'VN', 'PH', 'MY', 'PK', 'BD', 'SG', 'HK', 'TW', 'NP', 'LK', 'MM', 'KH', 'KZ'
    ]:
        return 'Asia'
    elif country in [
        'ZA', 'NG', 'EG', 'KE', 'GH', 'DZ', 'MA', 'TN', 'ET', 'UG', 'SD', 'TZ', 'SN', 'CI', 'CM', 'ZW', 'ZM', 'AO', 'NA'
    ]:
        return 'Africa'
    elif country in ['AU', 'NZ', 'FJ', 'PG', 'WS', 'TO', 'TV', 'SB', 'VU']:
        return 'Oceania'
    else:
        return 'Other'


df['region'] = df['country'].apply(map_region)

# --- Keep only final columns ---
final = df[['normalized_location', 'longitude', 'latitude', 'confidence', 'region']]

# --- Save new CSV ---
final_path = r'C:\Users\Admin\Documents\subjects\DBL project\data DBL\geocoded_with_region.csv'
final.to_csv(final_path, index=False)
print(f"Region-tagged CSV saved to: {final_path}")
