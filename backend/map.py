import pandas as pd
import folium

# Load geocoded data
data = pd.read_csv("../data/taxpayers_geo.csv")

# Center map around Lagos
map_center = [6.5244, 3.3792]

m = folium.Map(location=map_center, zoom_start=11)

# Add markers
for _, row in data.iterrows():
    if pd.notnull(row["latitude"]) and pd.notnull(row["longitude"]):
        folium.Marker(
            location=[row["latitude"], row["longitude"]],
            popup=f"{row['name']} | Tax Paid: ₦{row['tax_paid']}",
        ).add_to(m)

# Save map
m.save("tax_map.html")

print(" Tax map created successfully!")