# Cleaned_data.py
import os
import geopandas as gpd
import pandas as pd
import random
import numpy as np

# ---------------------------
# Paths
# ---------------------------
script_dir = os.path.dirname(os.path.abspath(__file__))
raw_file = os.path.join(script_dir, "../data/raw/abidjan_pois.geojson")
processed_csv = os.path.join(script_dir, "../data/processed/abidjan_cleaned.csv")
processed_geojson = os.path.join(script_dir, "../data/processed/abidjan_cleaned.geojson")

# ---------------------------
# Load raw data
# ---------------------------
if os.path.isfile(raw_file):
    print("File found! Loading data...")
    gdf = gpd.read_file(raw_file)
    print("Original rows:", len(gdf))
else:
    raise FileNotFoundError(f"File not found: {raw_file}")

# ---------------------------
# Initial cleaning
# ---------------------------
columns_to_keep = ['name', 'amenity', 'shop', 'tourism', 'osm_id', 'osm_type', 'geometry', 'opening_hours']
gdf_clean = gdf[columns_to_keep]
gdf_clean = gdf_clean.dropna(subset=['amenity'])
gdf_clean = gdf_clean.drop_duplicates()
gdf_clean = gdf_clean.reset_index(drop=True)

# Fill missing values
gdf_clean['name'] = gdf_clean['name'].fillna('Unknown')
gdf_clean['shop'] = gdf_clean['shop'].fillna('None')
gdf_clean['tourism'] = gdf_clean['tourism'].fillna('None')

# ---------------------------
# Add dummy reviews & ratings
# ---------------------------
random.seed(42)
np.random.seed(42)

gdf_clean['reviews'] = [random.randint(0, 500) for _ in range(len(gdf_clean))]
gdf_clean['rating'] = [round(random.uniform(1, 5), 1) for _ in range(len(gdf_clean))]

# ---------------------------
# Replace 'None' strings with actual NA
# ---------------------------
df = gdf_clean.copy()
df["amenity"] = df["amenity"].replace("None", pd.NA)
df["shop"] = df["shop"].replace("None", pd.NA)
df["tourism"] = df["tourism"].replace("None", pd.NA)

# ---------------------------
# Define business / non-business amenities
# ---------------------------
business_amenities = [
    'fuel', 'car_wash', 'bureau_de_change', 'restaurant', 'bank', 'food_court',
    'pub', 'clinic', 'pharmacy', 'doctors', 'money_transfer', 'nightclub',
    'bar', 'marketplace', 'driving_school', 'ice_cream', 'atm', 'internet_cafe',
    'cafe', 'fast_food', 'motorcycle_repair', 'garage auto', 'veterinary',
    'car_rental', 'bicycle_repair_station', 'stripclub', 'studio', 'boat_rental',
    'coworking_space', 'cinema', 'dentist', 'brothel', 'casino',
    'mobile_money_agent', 'shipping', 'car_sharing', 'microfinance_bank',
    'theatre', 'music_school', 'conference_centre', "O'TOPAZ, Pâtisserie",
    'charging_station', 'cars', 'tattoos', 'Pressing', 'animal_breeding', 'taxi', 'parking',
    'parking_space', 'motorcycle_parking'
]

# Drop meaningless shop values
df["shop"] = df["shop"].replace({"yes": pd.NA, "no": pd.NA})

# Handle tourism
include_attraction = False  # True to keep attractions
business_tourism = ["hotel"] if not include_attraction else ["hotel", "attraction"]

# ---------------------------
# Create unified business_type column
# ---------------------------
def get_business_type(row):
    if pd.notna(row["shop"]):
        return row["shop"]
    elif pd.notna(row["amenity"]) and row["amenity"] in business_amenities:
        return row["amenity"]
    elif pd.notna(row["tourism"]) and row["tourism"] in business_tourism:
        return row["tourism"]
    else:
        return None

df["business_type"] = df.apply(get_business_type, axis=1)

# Drop rows without a business type
df = df.dropna(subset=["business_type"])

# ---------------------------
# Merge / fix business types
# ---------------------------
mapping = {
    'bakery': 'restaurant', 'pastry': 'restaurant', 'Maquis kaplin': 'restaurant',
    "O'TOPAZ, Pâtisserie": 'restaurant', 'seafood': 'restaurant',
    'coffee': 'cafe', 'Kiosque café': 'cafe', 'Buvette traditionnelle': 'cafe',
    'alcohol': 'bar', 'beverages': 'bar',
    'chemist': 'pharmacy', 'optician': 'pharmacy',
    'copyshop': 'internet_cafe', 'computer': 'internet_cafe',
    'microfinance_bank': 'bank', 'orange money': 'mobile_money_agent',
    'hardware': 'marketplace', 'jewelry': 'marketplace', 'supermarket': 'marketplace',
    'tattoo': 'studio', 'music_school': 'studio',
    'theatre': 'cinema', 'car_sharing': 'car_rental',
    'motorcycle_parking': 'parking', 'parking_space': 'parking',
    'beauty': 'clinic', 'animal_breeding': 'veterinary'
}

to_drop = [
    'casino', 'brothel', 'stripclub', 'religion', 'car', 'music', 'shipping',        
    'dry_cleaning', 'funeral_directors', 'coworking_space', 'conference_centre',
    'charging_station', 'boat_rental',
]

df['business_type'] = df['business_type'].replace(mapping)
df = df[~df['business_type'].isin(to_drop)]

# ---------------------------
# Handle opening_hours & calculate duration
# ---------------------------
import re
import datetime
import random

# --- Simplify existing opening_hours ---
def simplify_opening_hours(value):
    if pd.isna(value):
        return None

    value = str(value).strip()

    # 24/7 patterns
    if re.search(r'24/?7|00:00-24:00|00:00-00:00', value):
        return "00:00-23:59"

    # Closed or invalid
    if re.search(r'closed|off|unknown|n/a', value, re.IGNORECASE):
        return None

    # Extract HH:MM-HH:MM
    matches = re.findall(r'(\d{1,2}:\d{2})-(\d{1,2}:\d{2})', value)
    if matches:
        opens = [m[0] for m in matches]
        closes = [m[1] for m in matches]
        return f"{sorted(opens)[0]}-{sorted(closes)[-1]}"

    # Single time like "08:18"
    single = re.match(r'^\d{1,2}:\d{2}$', value)
    if single:
        t = single.group(0)
        return f"{t}-{t}"

    return None

df['opening_hours'] = df['opening_hours'].apply(simplify_opening_hours)

# --- Fill missing opening_hours with random values by business_type ---
opening_hours_range = {
    "restaurant": [(10, 12), (21, 23)],
    "pub": [(16, 20), (0, 2)],
    "money_transfer": [(8, 10), (16, 18)],
    "cafe": [(6, 8), (20, 23)],
    "pharmacy": [(8, 10), (20, 23)],
    "bank": [(8, 9), (14, 16)],
    "fuel": [(0, 0), (23, 23)],  # 24/7
    "doctors": [(9, 11), (16, 19)],
    "bar": [(17, 21), (1, 3)],
    "car_wash": [(8, 9), (17, 19)],
    "internet_cafe": [(9, 11), (22, 0)],
    "marketplace": [(7, 9), (16, 18)],
    "clinic": [(9, 11), (17, 19)],
    "fast_food": [(11, 13), (22, 0)],
    "driving_school": [(9, 11), (15, 17)],
    "nightclub": [(21, 23), (3, 6)],
    "food_court": [(10, 12), (22, 0)],
    "ice_cream": [(11, 13), (20, 22)],
    "parking": [(0, 0), (23, 23)],  # 24/7
    "atm": [(0, 0), (23, 23)],      # 24/7
    "bicycle_repair_station": [(9, 11), (17, 19)],
    "dentist": [(9, 11), (17, 19)],
    "car_rental": [(8, 10), (18, 20)],
    "bureau_de_change": [(9, 11), (17, 19)],
    "veterinary": [(9, 11), (17, 19)],
    "mobile_money_agent": [(8, 10), (17, 19)],
    "studio": [(10, 12), (20, 22)],
    "motorcycle_repair": [(8, 10), (17, 19)],
    "cinema": [(14, 16), (23, 1)],
}

def random_opening_hours(btype):
    if btype not in opening_hours_range:
        return "09:00-18:00"

    (open_start, open_end), (close_start, close_end) = opening_hours_range[btype]

    # Handle 24/7
    if open_start == 0 and open_end == 0 and close_start == 23 and close_end == 23:
        return "00:00-23:59"

    # Pick random open and close hours within ranges
    if open_start <= open_end:
        open_hour = random.randint(open_start, open_end)
    else:  # wraparound
        open_hour = random.choice(list(range(open_start, 24)) + list(range(0, open_end+1)))

    if close_start <= close_end:
        close_hour = random.randint(close_start, close_end)
    else:  # wraparound
        close_hour = random.choice(list(range(close_start, 24)) + list(range(0, close_end+1)))

    # Ensure closing is after opening
    open_dt = datetime.datetime.combine(datetime.date.today(), datetime.time(hour=open_hour))
    close_dt = datetime.datetime.combine(datetime.date.today(), datetime.time(hour=close_hour))
    if close_dt <= open_dt:
        close_dt += datetime.timedelta(hours=random.randint(3, 6))  # random duration 3–6h
        if close_dt.day > open_dt.day:  # past midnight
            close_hour = close_dt.hour

    return f"{open_hour:02d}:00-{close_hour:02d}:00"


df['opening_hours'] = df.apply(
    lambda row: row['opening_hours'] if pd.notna(row['opening_hours']) else random_opening_hours(row['business_type']),
    axis=1
)

# --- Convert opening_hours to open_time, close_time, and duration_hours ---
df['opening_hours'] = df['opening_hours'].str.replace("24:00", "00:00")
df[['open_str','close_str']] = df['opening_hours'].str.split('-', expand=True)

df['open_time'] = pd.to_datetime(df['open_str'], format="%H:%M", errors='coerce').dt.time
df['close_time'] = pd.to_datetime(df['close_str'], format="%H:%M", errors='coerce').dt.time

def calculate_duration(open_t, close_t):
    open_dt = datetime.datetime.combine(datetime.date.today(), open_t)
    close_dt = datetime.datetime.combine(datetime.date.today(), close_t)
    if close_dt <= open_dt:
        close_dt += datetime.timedelta(days=1)
    return (close_dt - open_dt).seconds / 3600

df['duration_hours'] = df.apply(lambda row: calculate_duration(row['open_time'], row['close_time']), axis=1)

# Drop helper columns
df.drop(columns=['open_str','close_str'], inplace=True)


# ---------------------------
# Save cleaned data
# ---------------------------
os.makedirs(os.path.join(script_dir, "../data/processed"), exist_ok=True)
df.to_csv(processed_csv, index=False)
df.to_file(processed_geojson, driver="GeoJSON")

# ---------------------------
# Summary
# ---------------------------
print("Remaining rows after filtering:", len(df))
print(df['business_type'].value_counts())
print(df.head())
