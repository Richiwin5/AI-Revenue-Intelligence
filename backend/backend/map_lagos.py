import pandas as pd
import folium
import requests
import json

# ----------------------------
# 1️⃣ Load taxpayer data
# ----------------------------
data = pd.read_csv("../data/taxpayers_geo.csv")

# ----------------------------
# 2️⃣ Download Lagos LGA GeoJSON
# ----------------------------
with open("../data/lagos_lga.geojson") as f:
   lagos_geo = json.load(f)



# ----------------------------
# 3️⃣ Create Map
# ----------------------------
m = folium.Map(location=[6.5244, 3.3792], zoom_start=10)

# Add Lagos LGA boundaries
folium.GeoJson(
    lagos_geo,
    name="Lagos LGAs",
    style_function=lambda x: {
        "fillColor": "transparent",
        "color": "blue",
        "weight": 2,
    },
).add_to(m)

# ----------------------------
# 4️⃣ Add taxpayer markers
# ----------------------------
for _, row in data.iterrows():
    if pd.notnull(row["latitude"]) and pd.notnull(row["longitude"]):
        folium.Marker(
            location=[row["latitude"], row["longitude"]],
            popup=f"{row['name']} | ₦{row['tax_paid']}"
        ).add_to(m)

# ----------------------------
# 5️⃣ Save map
# ----------------------------
m.save("tax_map.html")
print("✅ Lagos tax map created! Open 'tax_map.html' to view it.")




# import pandas as pd
# import folium
# import requests
# import json

# # ----------------------------
# # 1️⃣ Load taxpayer data
# # ----------------------------
# data = pd.read_csv("../data/taxpayers_geo.csv")

# # ----------------------------
# # 2️⃣ Download Lagos LGA GeoJSON
# # ----------------------------
# with open("../data/lagos_lga.geojson") as f:
#     lagos_geo = json.load(f)

# # ----------------------------
# # 3️⃣ Create Map
# # ----------------------------
# m = folium.Map(location=[6.5244, 3.3792], zoom_start=10)

# # Add Lagos LGA boundaries
# folium.GeoJson(
#     lagos_geo,
#     name="Lagos LGAs",
#     style_function=lambda x: {
#         "fillColor": "transparent",
#         "color": "blue",
#         "weight": 2,
#     },
# ).add_to(m)

# # ----------------------------
# # 4️⃣ Add taxpayer markers
# # ----------------------------
# for _, row in data.iterrows():
#     if pd.notnull(row["latitude"]) and pd.notnull(row["longitude"]):
#         folium.Marker(
#             location=[row["latitude"], row["longitude"]],
#             popup=f"{row['name']} | ₦{row['tax_paid']}"
#         ).add_to(m)

# # ----------------------------
# # 5️⃣ Save map
# # ----------------------------
# m.save("tax_map.html")
# print("✅ Lagos tax map created! Open 'tax_map.html' to view it.")