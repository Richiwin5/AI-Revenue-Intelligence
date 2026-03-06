# backend/generate_lga_map.py

import geopandas as gpd
import pandas as pd
import folium
import psycopg2

# -------------------- PostgreSQL Connection --------------------
def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="lagos_revenue",
        user="iwebukeonyeamachi",
        password="Richiwin"  
    )

# -------------------- Map Creation --------------------
def create_map():
    """
    Generates an interactive Lagos LGA tax map (HTML) using:
    - lagos_lga.geojson for boundaries
    - PostgreSQL tax_records + taxpayers for data
    """

    # 1️⃣ Load GeoJSON
    lagos_geo = gpd.read_file("../data/lagos_lga.geojson")

    # 2️⃣ Fetch LGA tax summary from PostgreSQL
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT t.lga, SUM(tr.expected_tax) as total_tax,
               AVG(t.compliance_score) as avg_compliance
        FROM tax_records tr
        JOIN taxpayers t ON tr.taxpayer_id = t.id
        GROUP BY t.lga
    """)
    rows = cur.fetchall()
    lga_tax_df = pd.DataFrame(rows, columns=["lga", "total_tax", "avg_compliance"])
    cur.close()
    conn.close()

    # 3️⃣ Merge GeoJSON with tax data
    # Ensure 'LGA_NAME' in GeoJSON matches 'lga' column
    lagos_geo = lagos_geo.merge(lga_tax_df, left_on="LGA_NAME", right_on="lga", how="left")
    lagos_geo["total_tax"] = lagos_geo["total_tax"].fillna(0)
    lagos_geo["avg_compliance"] = lagos_geo["avg_compliance"].fillna(0)

    # 4️⃣ Create folium map
    m = folium.Map(location=[6.5244, 3.3792], zoom_start=10)  # Centered on Lagos

    # 5️⃣ Choropleth layer
    folium.Choropleth(
        geo_data=lagos_geo,
        name="choropleth",
        data=lagos_geo,
        columns=["LGA_NAME", "total_tax"],
        key_on="feature.properties.LGA_NAME",
        fill_color="YlGnBu",
        fill_opacity=0.7,
        line_opacity=0.2,
        legend_name="Total Tax Collected (₦)"
    ).add_to(m)

    # 6️⃣ Add popups for each LGA
    for _, row in lagos_geo.iterrows():
        folium.Marker(
            location=[row.geometry.centroid.y, row.geometry.centroid.x],
            popup=(
                f"<b>{row['LGA_NAME']}</b><br>"
                f"Total Tax: ₦{row['total_tax']:,.0f}<br>"
                f"Avg Compliance: {row['avg_compliance']:.2f}%"
            )
        ).add_to(m)

    # 7️Save map to HTML
    m.save("../data/tax_map.html")
    print(" LGA tax map generated: data/tax_map.html")