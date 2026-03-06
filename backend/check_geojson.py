import json

with open("../data/nigeria_lga.geojson") as f:
    data = json.load(f)

# print first feature properties
print(data["features"][0]["properties"])