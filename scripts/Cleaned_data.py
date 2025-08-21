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
columns_to_keep = ['name', 'amenity', 'shop', 'tourism', 'osm_id', 'osm_type', 'geometry']
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
