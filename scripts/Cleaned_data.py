import os
import geopandas as gpd
import random

script_dir = os.path.dirname(os.path.abspath(__file__))

# Define paths for raw data and cleaned outputs
raw_file = os.path.join(script_dir, "../data/raw/abidjan_pois.geojson")
clean_csv_file = os.path.join(script_dir, "../data/processed/abidjan_pois_cleaned.csv")
clean_geojson_file = os.path.join(script_dir, "../data/processed/abidjan_pois_cleaned.geojson")

# Check if raw file exists
if os.path.isfile(raw_file):
    print("File found! Loading data...")
    gdf = gpd.read_file(raw_file)
    print("Original rows:", len(gdf))
    print("Columns:", gdf.columns)
else:
    raise FileNotFoundError(f"File not found. Check the path: {raw_file}")

# Cleaning process

# Keep only relevant columns
columns_to_keep = ['name', 'amenity', 'shop', 'tourism', 'osm_id', 'osm_type', 'geometry']
gdf_clean = gdf[columns_to_keep]

# Drop rows where essential columns are missing
gdf_clean = gdf_clean.dropna(subset=['name', 'amenity'])

# Remove exact duplicates
gdf_clean = gdf_clean.drop_duplicates()

# Reset index
gdf_clean = gdf_clean.reset_index(drop=True)

# Fill optional fields with 'None'
gdf_clean['shop'] = gdf_clean['shop'].fillna('None')
gdf_clean['tourism'] = gdf_clean['tourism'].fillna('None')

# Add dummy ratings/reviews
gdf_clean['reviews'] = [random.randint(0, 500) for _ in range(len(gdf_clean))]
gdf_clean['rating'] = [round(random.uniform(1, 5), 1) for _ in range(len(gdf_clean))]

# Final checks
print("Number of rows after cleaning:", len(gdf_clean))
print("Missing values per column:\n", gdf_clean.isnull().sum())
print("Columns in cleaned data:\n", gdf_clean.columns)
print("Sample data:\n", gdf_clean[['name', 'amenity', 'reviews', 'rating']].head())

# Save cleaned data

# CSV
gdf_clean.to_csv(clean_csv_file, index=False)
print(f"Cleaned CSV saved to: {clean_csv_file}")

# GeoJSON
gdf_clean.to_file(clean_geojson_file, driver='GeoJSON')
print(f"Cleaned GeoJSON saved to: {clean_geojson_file}")
