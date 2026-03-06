from geopy.geocoders import Nominatim
import pandas as pd

geo = Nominatim(user_agent="tax_ai")

data = pd.read_csv("../data/taxpayers.csv")

latitudes = []
longitudes = []

for address in data["address"]:
    location = geo.geocode(address)
    
    if location:
        latitudes.append(location.latitude)
        longitudes.append(location.longitude)
    else:
        latitudes.append(None)
        longitudes.append(None)

data["latitude"] = latitudes
data["longitude"] = longitudes

data.to_csv("../data/taxpayers_geo.csv", index=False)

print("Geocoding complete!")