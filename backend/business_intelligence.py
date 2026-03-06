import os
import pandas as pd
from sqlalchemy import create_engine
import folium
from branca.element import Template, MacroElement
import numpy as np

# =========================================================
# Display large numbers nicely
# =========================================================
pd.set_option('display.float_format', '{:,.0f}'.format)

# =========================================================
# 0️⃣ Database Connection
# =========================================================
DB_USER = os.getenv("DB_USER", "iwebukeonyeamachi")
DB_PASS = os.getenv("DB_PASS", "Richiwin")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "lagos_revenue")

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")
print("✅ Connected to Lagos Revenue Database")

# =========================================================
# 1️⃣ Load LGAs from CSV (for local govt matching)
# =========================================================
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(current_dir, "../data/lagos_lgas.csv")

    lagos_lga = pd.read_csv(csv_path)
    LGA_LIST = [lga.strip().lower() for lga in lagos_lga["lga_name"].tolist()]
    print("✅ Loaded LGAs successfully from CSV")

except Exception as e:
    print("⚠️ Error loading lagos_lgas.csv:", e)
    lagos_lga = pd.DataFrame(columns=['lga_name','latitude','longitude'])
    LGA_LIST = []

# =========================================================
# 2️⃣ Load Taxpayers, Businesses, Properties
# =========================================================
taxpayers = pd.read_sql("SELECT * FROM taxpayers", engine)
businesses = pd.read_sql("SELECT * FROM businesses", engine)
properties = pd.read_sql("SELECT * FROM properties", engine)

# =========================================================
# 3️⃣ Compute Total Income by LGA
# =========================================================
taxpayer_income = taxpayers.groupby('lga')['declared_income'].sum().reset_index().rename(columns={'declared_income':'taxpayer_income'})
business_income = businesses.groupby('lga')['annual_revenue'].sum().reset_index().rename(columns={'annual_revenue':'business_income'})
property_income = properties.groupby('lga')['estimated_value'].sum().reset_index().rename(columns={'estimated_value':'property_income'})

summary = lagos_lga[['lga_name','latitude','longitude']].merge(
    taxpayer_income, left_on='lga_name', right_on='lga', how='left'
).merge(
    business_income, left_on='lga_name', right_on='lga', how='left'
).merge(
    property_income, left_on='lga_name', right_on='lga', how='left'
)

summary[['taxpayer_income','business_income','property_income']] = summary[['taxpayer_income','business_income','property_income']].fillna(0)
summary['total_income'] = summary['taxpayer_income'] + summary['business_income'] + summary['property_income']

# =========================================================
# 4️⃣ Summaries
# =========================================================
highest = summary.loc[summary['total_income'].idxmax()]
lowest = summary.loc[summary['total_income'].idxmin()]

print("\n📊 LGA Revenue Summary:")
print(summary[['lga_name','total_income']])
print(f"\n🏆 Highest Paying LGA: {highest['lga_name']} ({highest['total_income']:,} NGN)")
print(f"⚠️ Lowest Paying LGA: {lowest['lga_name']} ({lowest['total_income']:,} NGN)")

# =========================================================
# 5️⃣ Revenue by Occupation
# =========================================================
business_summary = taxpayers.groupby('occupation')['declared_income'].sum().reset_index().rename(columns={'declared_income':'total_income'})
business_summary = business_summary.sort_values(by='total_income', ascending=False)
growth_rate = 0.15
business_summary['projected_next_year'] = business_summary['total_income'] * (1 + growth_rate)

top_business = business_summary.iloc[0]
print(f"\n🧠 Top Revenue Business Type: {top_business['occupation']} ({top_business['total_income']:,} NGN)")

# =========================================================
# 6️⃣ Interactive Map
# =========================================================
taxpayers_map = taxpayers.merge(
    lagos_lga[['lga_name','latitude','longitude']],
    left_on='lga',
    right_on='lga_name',
    how='left'
).dropna(subset=['latitude','longitude'])

m = folium.Map(location=[6.5244,3.3792], zoom_start=10)
for _, row in taxpayers_map.iterrows():
    color = "blue"
    if row['lga'] == highest['lga_name']:
        color = "green"
    if row['occupation'] == top_business['occupation']:
        color = "orange"
    if row['lga'] == highest['lga_name'] and row['occupation'] == top_business['occupation']:
        color = "red"

    folium.CircleMarker(
        location=[row['latitude'], row['longitude']],
        radius=6,
        color=color,
        fill=True,
        fill_opacity=0.7,
        popup=f"{row['full_name']} | Income: ₦{row['declared_income']:,} | {row['occupation']} | LGA: {row['lga']}"
    ).add_to(m)

# LGA markers
for _, row in lagos_lga.iterrows():
    if pd.notna(row['latitude']) and pd.notna(row['longitude']):
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            icon=folium.Icon(color='red'),
            popup=row['lga_name']
        ).add_to(m)

# Legend
template = """
{% macro html(this, kwargs) %}
<div style="
    position: fixed;
    bottom: 50px;
    left: 50px;
    width: 230px;
    z-index:9999;
    background-color:white;
    border:2px solid grey;
    border-radius:5px;
    padding: 10px;
    font-size:14px;">
<b>Legend:</b><br>
<i class="fa fa-circle" style="color:red"></i> Top LGA + Top Business Type<br>
<i class="fa fa-circle" style="color:green"></i> Top LGA<br>
<i class="fa fa-circle" style="color:orange"></i> Top Business Type<br>
<i class="fa fa-circle" style="color:blue"></i> Other Businesses
</div>
{% endmacro %}
"""
macro = MacroElement()
macro._template = Template(template)
m.get_root().add_child(macro)

m.save("../data/business_lga_map.html")
print("\n🗺️ Interactive map saved: business_lga_map.html")

# =========================================================
# 7️⃣ Government Recommendations
# =========================================================
print("\n🏛 Government Recommendations:")
print(f"- Focus policy incentives on {top_business['occupation']} sector.")
print(f"- Investigate low-performing LGA: {lowest['lga_name']}.")
print(f"- Strengthen compliance in high-performing LGA: {highest['lga_name']}.")
print("\n🚀 Lagos Revenue Intelligence Analysis Complete.")


# import os
# import pandas as pd
# import geopandas as gpd
# from sqlalchemy import create_engine
# import folium
# from branca.element import Template, MacroElement

# # =========================================================
# # 0️⃣ Database Connection
# # =========================================================

# # Optional: use environment variables for production
# DB_USER = os.getenv("DB_USER", "iwebukeonyeamachi")  # fallback for local testing
# DB_PASS = os.getenv("DB_PASS", "Richiwin")
# DB_HOST = os.getenv("DB_HOST", "localhost")
# DB_NAME = os.getenv("DB_NAME", "lagos_revenue")

# engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")
# print(" Connected to Lagos Revenue Database")

# # =========================================================
# # 1️⃣ Load LGAs
# # =========================================================

# # Try loading from DB; fallback to CSV if table does not exist
# try:
#     lagos_lga = pd.read_sql("SELECT * FROM lagos_lgas", engine)
#     print(" Loaded LGAs from database")
# except Exception as e:
#     print("⚠️ lagos_lgas table not found, loading from CSV")
#     lagos_lga = pd.read_csv("../data/lagos_lgas.csv")

# # Convert to GeoDataFrame
# lagos_gdf = gpd.GeoDataFrame(
#     lagos_lga,
#     geometry=gpd.points_from_xy(lagos_lga.longitude, lagos_lga.latitude),
#     crs="EPSG:4326"
# )

# # =========================================================
# # 2️⃣ Load Businesses / Taxpayers
# # =========================================================

# # Fetch from database
# taxpayers = pd.read_sql("SELECT * FROM taxpayers", engine)

# # Make sure we have latitude/longitude columns; if not, generate dummy values for testing
# if 'latitude' not in taxpayers.columns or 'longitude' not in taxpayers.columns:
#     # This is temporary for testing
#     import numpy as np
#     taxpayers['latitude'] = 6.5244 + np.random.rand(len(taxpayers))/10
#     taxpayers['longitude'] = 3.3792 + np.random.rand(len(taxpayers))/10

# business_gdf = gpd.GeoDataFrame(
#     taxpayers,
#     geometry=gpd.points_from_xy(taxpayers.longitude, taxpayers.latitude),
#     crs="EPSG:4326"
# )

# # =========================================================
# # 3️⃣ Project CRS to meters for accurate distance
# # =========================================================

# lagos_gdf_proj = lagos_gdf.to_crs(epsg=3857)
# business_gdf_proj = business_gdf.to_crs(epsg=3857)

# # =========================================================
# # 4️⃣ Assign taxpayers to nearest LGA
# # =========================================================

# joined_proj = gpd.sjoin_nearest(
#     business_gdf_proj,
#     lagos_gdf_proj[['lga_name', 'geometry']],
#     how="left",
#     distance_col="distance_m"
# )

# joined = joined_proj.to_crs(epsg=4326)

# # =========================================================
# # 5️⃣ Revenue Summary by LGA
# # =========================================================

# summary = joined.groupby('lga_name')['declared_income'].sum().reset_index()
# summary.rename(columns={'declared_income':'total_income'}, inplace=True)

# highest = summary.loc[summary['total_income'].idxmax()]
# lowest = summary.loc[summary['total_income'].idxmin()]

# print("\n📊 LGA Revenue Summary:")
# print(summary)
# print(f"\n🏆 Highest Paying LGA: {highest['lga_name']} ({highest['total_income']:,} NGN)")
# print(f"⚠️ Lowest Paying LGA: {lowest['lga_name']} ({lowest['total_income']:,} NGN)")

# # =========================================================
# # 6️⃣ Revenue by Occupation / Business Type
# # =========================================================

# # Assuming 'occupation' as business type
# business_summary = (
#     joined.groupby('occupation')['declared_income']
#     .sum()
#     .reset_index()
#     .sort_values(by='declared_income', ascending=False)
# )
# business_summary.rename(columns={'declared_income':'total_income'}, inplace=True)
# top_business = business_summary.iloc[0]

# print(f"\n🧠 Top Revenue Business Type: {top_business['occupation']} ({top_business['total_income']:,} NGN)")

# # =========================================================
# # 7️⃣ Future Projection (15% Growth)
# # =========================================================

# growth_rate = 0.15
# business_summary['projected_next_year'] = business_summary['total_income'] * (1 + growth_rate)
# print("\n📈 Projected Revenue Next Year:")
# print(business_summary)

# # =========================================================
# # 8️⃣ Interactive Map
# # =========================================================

# m = folium.Map(location=[6.5244, 3.3792], zoom_start=10)

# for _, row in joined.iterrows():
#     color = "blue"
#     if row['lga_name'] == highest['lga_name']:
#         color = "green"
#     if row['occupation'] == top_business['occupation']:
#         color = "orange"
#     if row['lga_name'] == highest['lga_name'] and row['occupation'] == top_business['occupation']:
#         color = "red"

#     folium.CircleMarker(
#         location=[row['latitude'], row['longitude']],
#         radius=6,
#         color=color,
#         fill=True,
#         fill_opacity=0.7,
#         popup=(
#             f"{row['full_name']} | "
#             f"Income: {row['declared_income']} NGN | "
#             f"{row['occupation']} | "
#             f"LGA: {row['lga_name']}"
#         )
#     ).add_to(m)

# # LGA markers
# for _, row in lagos_gdf.iterrows():
#     folium.Marker(
#         location=[row['latitude'], row['longitude']],
#         icon=folium.Icon(color='red'),
#         popup=row['lga_name']
#     ).add_to(m)

# # Custom legend
# template = """
# {% macro html(this, kwargs) %}
# <div style="
#     position: fixed;
#     bottom: 50px;
#     left: 50px;
#     width: 230px;
#     z-index:9999;
#     background-color:white;
#     border:2px solid grey;
#     border-radius:5px;
#     padding: 10px;
#     font-size:14px;">
# <b>Legend:</b><br>
# <i class="fa fa-circle" style="color:red"></i> Top LGA + Top Business Type<br>
# <i class="fa fa-circle" style="color:green"></i> Top LGA<br>
# <i class="fa fa-circle" style="color:orange"></i> Top Business Type<br>
# <i class="fa fa-circle" style="color:blue"></i> Other Businesses
# </div>
# {% endmacro %}
# """
# macro = MacroElement()
# macro._template = Template(template)
# m.get_root().add_child(macro)

# m.save("../data/business_lga_map.html")
# print("\n Interactive map saved: business_lga_map.html")

# # =========================================================
# # 9️⃣ Government Intelligence Recommendations
# # =========================================================

# print("\n Government Recommendations:")
# print(f"- Focus policy incentives on {top_business['occupation']} sector.")
# print(f"- Investigate low-performing LGA: {lowest['lga_name']}.")
# print(f"- Strengthen compliance in high-performing LGA: {highest['lga_name']}.")
# print("\n Lagos Revenue Intelligence Analysis Complete.")




# import pandas as pd
# import geopandas as gpd
# from shapely.geometry import Point
# import folium
# from branca.element import Template, MacroElement

# # --------------------------
# # 1. Load Lagos LGAs
# # --------------------------
# lagos_lga = pd.read_csv("../datalagos_lgas.csv")

# lagos_gdf = gpd.GeoDataFrame(
#     lagos_lga,
#     geometry=gpd.points_from_xy(lagos_lga.longitude, lagos_lga.latitude),
#     crs="EPSG:4326"
# )

# # --------------------------
# # 2. Load Businesses
# # --------------------------
# businesses = pd.read_csv("../data/taxpayers_geo.csv")

# business_gdf = gpd.GeoDataFrame(
#     businesses,
#     geometry=gpd.points_from_xy(businesses.longitude, businesses.latitude),
#     crs="EPSG:4326"
# )

# # --------------------------
# # 3. Project CRS for accurate distance
# # --------------------------
# # EPSG:3857 = Web Mercator in meters
# lagos_gdf_proj = lagos_gdf.to_crs(epsg=3857)
# business_gdf_proj = business_gdf.to_crs(epsg=3857)

# # --------------------------
# # 4. Assign Businesses to Nearest LGA
# # --------------------------
# joined_proj = gpd.sjoin_nearest(
#     business_gdf_proj,
#     lagos_gdf_proj[['lga_name', 'geometry']],
#     how="left",
#     distance_col="distance"
# )

# # Convert back to lat/lon for mapping
# joined = joined_proj.to_crs(epsg=4326)

# # Save assignment
# joined.drop(columns=['geometry','index_right'], inplace=True)
# joined.to_csv("../data/taxpayers_with_lga.csv", index=False)
# print("✅ Businesses assigned to LGAs: taxpayers_with_lga.csv")

# # --------------------------
# # 5. LGA Tax Summary
# # --------------------------
# summary = joined.groupby('lga_name')['tax_paid'].sum().reset_index()
# summary.to_csv("../data/lga_tax_summary.csv", index=False)
# print("\n📊 LGA Tax Summary:")
# print(summary)

# # Highest and Lowest Paying LGA
# highest = summary.loc[summary['tax_paid'].idxmax()]
# lowest = summary.loc[summary['tax_paid'].idxmin()]

# print(f"\n🏆 Highest Paying LGA: {highest['lga_name']} ({highest['tax_paid']:,} NGN)")
# print(f"\n⚠️ Lowest Paying LGA: {lowest['lga_name']} ({lowest['tax_paid']:,} NGN)")

# # --------------------------
# # 6. Revenue by Business Type
# # --------------------------
# business_summary = (
#     joined.groupby('business_type')['tax_paid']
#     .sum()
#     .reset_index()
#     .sort_values(by='tax_paid', ascending=False)
# )
# business_summary.to_csv("../data/business_type_summary.csv", index=False)

# top_business = business_summary.iloc[0]
# print(f"\n🧠 Top Revenue Business Type: {top_business['business_type']} ({top_business['tax_paid']:,} NGN)")

# # --------------------------
# # 7. LGA + Business Type Breakdown
# # --------------------------
# lga_business = (
#     joined.groupby(['lga_name','business_type'])['tax_paid']
#     .sum()
#     .reset_index()
# )
# lga_business.to_csv("../data/lga_business_breakdown.csv", index=False)
# print("\n📍 LGA + Business Type Revenue Breakdown saved")

# # --------------------------
# # 8. Future Revenue Projection (15% Growth)
# # --------------------------
# growth_rate = 0.15
# business_summary['projected_next_year'] = business_summary['tax_paid'] * (1 + growth_rate)
# print("\n Projected Revenue Next Year by Business Type:")
# print(business_summary)

# # --------------------------
# # 9. Government Recommendations
# # --------------------------
# print("\n Government Recommendations:")
# print(f"- Focus on {top_business['business_type']} businesses for targeted revenue growth")
# print(f"- Invest more in the top-paying LGA: {highest['lga_name']}")

# # --------------------------
# # 10. Interactive Map (with legend)
# # --------------------------
# m = folium.Map(location=[6.5244, 3.3792], zoom_start=10)

# # Add businesses with color coding
# for _, row in joined.iterrows():
#     color = 'blue'
#     if row['lga_name'] == highest['lga_name']:
#         color = 'green'  # Top LGA
#     if row['business_type'] == top_business['business_type']:
#         color = 'orange'  # Top Business Type
#     if row['lga_name'] == highest['lga_name'] and row['business_type'] == top_business['business_type']:
#         color = 'red'  # Top LGA + Top Business Type

#     folium.CircleMarker(
#         location=[row['latitude'], row['longitude']],
#         radius=6,
#         color=color,
#         fill=True,
#         fill_opacity=0.7,
#         popup=f"{row['name']} | Tax: {row['tax_paid']} | {row['business_type']} | LGA: {row['lga_name']}"
#     ).add_to(m)

# # LGA markers
# for _, row in lagos_gdf.iterrows():
#     folium.Marker(
#         location=[row['latitude'], row['longitude']],
#         icon=folium.Icon(color='red'),
#         popup=row['lga_name']
#     ).add_to(m)

# # Add custom legend
# template = """
# {% macro html(this, kwargs) %}
# <div style="
#     position: fixed;
#     bottom: 50px;
#     left: 50px;
#     width: 200px;
#     height: 120px;
#     z-index:9999;
#     background-color:white;
#     border:2px solid grey;
#     border-radius:5px;
#     padding: 10px;
#     font-size:14px;
#     ">
# <b>Legend:</b><br>
# <i class="fa fa-circle" style="color:red"></i> Top LGA + Top Business Type<br>
# <i class="fa fa-circle" style="color:green"></i> Top LGA<br>
# <i class="fa fa-circle" style="color:orange"></i> Top Business Type<br>
# <i class="fa fa-circle" style="color:blue"></i> Other Businesses
# </div>
# {% endmacro %}
# """
# macro = MacroElement()
# macro._template = Template(template)
# m.get_root().add_child(macro)

# # Save map
# m.save("../data/business_lga_map.html")
# print("\n Interactive map saved with legend: business_lga_map.html")