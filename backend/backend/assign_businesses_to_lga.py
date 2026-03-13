import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import folium

# --------------------------
# 1. Load Lagos LGAs
# --------------------------
lagos_lga = pd.read_csv("lagos_lgas.csv")

lagos_gdf = gpd.GeoDataFrame(
    lagos_lga,
    geometry=gpd.points_from_xy(lagos_lga.longitude, lagos_lga.latitude),
    crs="EPSG:4326"
)

# --------------------------
# 2. Load Businesses
# --------------------------
businesses = pd.read_csv("../data/taxpayers_geo.csv")

business_gdf = gpd.GeoDataFrame(
    businesses,
    geometry=gpd.points_from_xy(businesses.longitude, businesses.latitude),
    crs="EPSG:4326"
)

# ✅ Project CRS for accurate distance
lagos_gdf = lagos_gdf.to_crs(epsg=3857)
business_gdf = business_gdf.to_crs(epsg=3857)

# --------------------------
# 3. Assign Businesses → LGA
# --------------------------
joined = gpd.sjoin_nearest(
    business_gdf,
    lagos_gdf[['lga_name', 'geometry']],
    how="left",
    distance_col="distance"
)

# Convert back
joined = joined.to_crs(epsg=4326)
lagos_gdf = lagos_gdf.to_crs(epsg=4326)

# ✅ CLEAN COLUMN NAMES
joined.rename(columns={'lga_name_right': 'lga_name'}, inplace=True)

if 'lga_name_left' in joined.columns:
    joined.drop(columns=['lga_name_left'], inplace=True)

# Remove geometry before saving
joined.drop(columns=['geometry','index_right'], inplace=True)

joined.to_csv("../data/taxpayers_with_lga.csv", index=False)
print("✅ Businesses assigned to LGAs")

# --------------------------
# 4. LGA Tax Summary
# --------------------------
summary = joined.groupby('lga_name')['tax_paid'].sum().reset_index()

summary.to_csv("../data/lga_tax_summary.csv", index=False)

print("\nLGA Tax Summary:")
print(summary)

# Highest
highest = summary.loc[summary['tax_paid'].idxmax()]
print("\n🏆 Highest Paying LGA:")
print(highest)

# Lowest
lowest = summary.loc[summary['tax_paid'].idxmin()]
print("\n⚠️ Lowest Paying LGA:")
print(lowest)


# 5. Interactive Map

m = folium.Map(location=[6.5244, 3.3792], zoom_start=10)

for _, row in joined.iterrows():
    folium.CircleMarker(
        location=[row['latitude'], row['longitude']],
        radius=5,
        color='blue',
        fill=True,
        fill_opacity=0.7,
        popup=f"{row['name']} | Tax: {row['tax_paid']} | {row['lga_name']}"
    ).add_to(m)

for _, row in lagos_gdf.iterrows():
    folium.Marker(
        location=[row['latitude'], row['longitude']],
        icon=folium.Icon(color='red'),
        popup=row['lga_name']
    ).add_to(m)

m.save("../data/business_lga_map.html")

print("✅ Interactive map saved")