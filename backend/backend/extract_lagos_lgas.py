import json

# Load full Nigeria LGA GeoJSON
with open("../data/nigeria_lga.geojson") as f:
    nigeria_geo = json.load(f)

# List of Lagos State LGAs
lagos_lgas = [
    "Agege", "Ajeromi-Ifelodun", "Alimosho", "Amuwo-Odofin", "Apapa",
    "Badagry", "Epe", "Eti-Osa", "Ibeju-Lekki", "Ifako-Ijaiye",
    "Ikeja", "Ikorodu", "Kosofe", "Lagos Island", "Lagos Mainland",
    "Mushin", "Ojo", "Oshodi-Isolo", "Shomolu", "Surulere"
]

lagos_features = [
    feature
    for feature in nigeria_geo["features"]
    if feature["properties"].get("LGA") in lagos_lgas
]

lagos_geojson = {
    "type": "FeatureCollection",
    "features": lagos_features
}

# Save only Lagos LGAs
with open("../data/lagos_lgas.geojson", "w") as out:
    json.dump(lagos_geojson, out)

print(" Extracted Lagos LGAs GeoJSON")