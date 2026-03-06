import geopandas as gpd
import pandas as pd

# Load the downloaded Nigeria boundaries GeoJSON
gdf = gpd.read_file("../data/nga_admin_boundaries/nga_admin2.geojson")

# Inspect columns (optional)
print(gdf.columns)
print(gdf.head())

# Filter LGAs for Lagos State
lagos_lga = gdf[(gdf['adm1_name'] == 'Lagos') & (gdf['adm2_name'].notnull())]

# Ensure the coordinate system is WGS84
lagos_lga = lagos_lga.to_crs(epsg=4326)

# Compute centroids for each LGA
centroids = lagos_lga.to_crs(3857).centroid.to_crs(4326)
lagos_lga['latitude'] = centroids.y
lagos_lga['longitude'] = centroids.x

# Prepare CSV with LGA name and coordinates
output = lagos_lga[['adm2_name', 'latitude', 'longitude']]
output.rename(columns={'adm2_name':'lga_name'}, inplace=True)

# Save CSV
output.to_csv("lagos_lgas.csv", index=False)
print("✅ Lagos LGA CSV created!")