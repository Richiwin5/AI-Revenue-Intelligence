# business_intelligence.py - Fixed merge operations
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import folium
from datetime import datetime

# =========================================================
# Display large numbers nicely
# =========================================================
pd.set_option('display.float_format', '{:,.0f}'.format)

def format_currency(val):
    """Format currency with trillion/billion/million support"""
    if pd.isna(val) or val == 0:
        return "₦0"
    if val >= 1_000_000_000_000:
        return f"₦{val/1_000_000_000_000:.2f}T"
    if val >= 1_000_000_000:
        return f"₦{val/1_000_000_000:.2f}B"
    if val >= 1_000_000:
        return f"₦{val/1_000_000:.2f}M"
    if val >= 1_000:
        return f"₦{val/1_000:.2f}K"
    return f"₦{val:,.0f}"

def format_percentage(val):
    """Format percentage"""
    return f"{val:.1f}%"

# =========================================================
# 0️⃣ Database Connection
# =========================================================
DB_USER = os.getenv("DB_USER", "iwebukeonyeamachi")
DB_PASS = os.getenv("DB_PASS", "Richiwin")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "lagos_revenue")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
)

print("✅ Connected to Lagos Revenue Database")

# =========================================================
# 1️⃣ Load LGAs
# =========================================================
current_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(current_dir, "../data/lagos_lgas.csv")

lagos_lga = pd.read_csv(csv_path)
print(f"✅ Loaded {len(lagos_lga)} LGAs")

# =========================================================
# 2️⃣ Load Data
# =========================================================
taxpayers = pd.read_sql("SELECT * FROM taxpayers", engine)
businesses = pd.read_sql("SELECT * FROM businesses", engine)
properties = pd.read_sql("SELECT * FROM properties", engine)
tax_records = pd.read_sql("SELECT * FROM tax_records", engine)

print(f"✅ Loaded {len(taxpayers)} taxpayers, {len(businesses)} businesses, {len(properties)} properties")

# =========================================================
# 3️⃣ GLOBAL REVENUE (SOURCE OF TRUTH)
# =========================================================
total_taxpayer_income = taxpayers["declared_income"].sum()
total_business_revenue = businesses["annual_revenue"].sum()
total_property_value = properties["estimated_value"].sum()
total_tax_paid = tax_records["tax_paid"].sum() if not tax_records.empty else 0
total_tax_expected = tax_records["expected_tax"].sum() if not tax_records.empty else 0

TOTAL_REVENUE = total_taxpayer_income + total_business_revenue + total_property_value

print(f"\n💰 TOTAL SYSTEM REVENUE: {format_currency(TOTAL_REVENUE)}")
print(f"   • Taxpayer Income: {format_currency(total_taxpayer_income)}")
print(f"   • Business Revenue: {format_currency(total_business_revenue)}")
print(f"   • Property Value: {format_currency(total_property_value)}")
print(f"   • Tax Collected: {format_currency(total_tax_paid)}")

# =========================================================
# 4️⃣ Revenue by LGA (Enhanced) - FIXED MERGE
# =========================================================
print("\n" + "="*60)
print("📊 LGA REVENUE ANALYSIS")
print("="*60)

# Calculate revenue by source per LGA
taxpayer_by_lga = taxpayers.groupby("lga")["declared_income"].sum().reset_index()
taxpayer_by_lga.rename(columns={"declared_income": "taxpayer_income", "lga": "lga_name"}, inplace=True)

business_by_lga = businesses.groupby("lga")["annual_revenue"].sum().reset_index()
business_by_lga.rename(columns={"annual_revenue": "business_revenue", "lga": "lga_name"}, inplace=True)

property_by_lga = properties.groupby("lga")["estimated_value"].sum().reset_index()
property_by_lga.rename(columns={"estimated_value": "property_value", "lga": "lga_name"}, inplace=True)

# Calculate counts per LGA
taxpayer_count_by_lga = taxpayers.groupby("lga").size().reset_index(name="taxpayer_count")
taxpayer_count_by_lga.rename(columns={"lga": "lga_name"}, inplace=True)

business_count_by_lga = businesses.groupby("lga").size().reset_index(name="business_count")
business_count_by_lga.rename(columns={"lga": "lga_name"}, inplace=True)

property_count_by_lga = properties.groupby("lga").size().reset_index(name="property_count")
property_count_by_lga.rename(columns={"lga": "lga_name"}, inplace=True)

# Start with base LGAs
summary = lagos_lga.copy()

# Merge one by one to avoid conflicts
summary = summary.merge(taxpayer_by_lga, on="lga_name", how="left")
summary = summary.merge(business_by_lga, on="lga_name", how="left")
summary = summary.merge(property_by_lga, on="lga_name", how="left")
summary = summary.merge(taxpayer_count_by_lga, on="lga_name", how="left")
summary = summary.merge(business_count_by_lga, on="lga_name", how="left")
summary = summary.merge(property_count_by_lga, on="lga_name", how="left")

# Fill NaN with 0
summary = summary.fillna(0)

# Calculate totals
summary["total_revenue"] = (
    summary["taxpayer_income"] +
    summary["business_revenue"] +
    summary["property_value"]
)

# Calculate averages
summary["avg_taxpayer_income"] = summary.apply(
    lambda x: x["taxpayer_income"] / x["taxpayer_count"] if x["taxpayer_count"] > 0 else 0, axis=1
)

summary["avg_business_revenue"] = summary.apply(
    lambda x: x["business_revenue"] / x["business_count"] if x["business_count"] > 0 else 0, axis=1
)

summary["avg_property_value"] = summary.apply(
    lambda x: x["property_value"] / x["property_count"] if x["property_count"] > 0 else 0, axis=1
)

# Sort by total revenue
summary = summary.sort_values("total_revenue", ascending=False)

# Display top 10 LGAs
print("\n📈 TOP 10 LGAS BY TOTAL REVENUE:")
print(summary[["lga_name", "total_revenue", "taxpayer_income", "business_revenue", "property_value"]].head(10).to_string(index=False))

# =========================================================
# 5️⃣ Highest and Lowest LGA with Insights
# =========================================================
highest = summary.iloc[0]
lowest = summary.iloc[-1]

print(f"\n🏆 HIGHEST PERFORMING LGA: {highest['lga_name']}")
print(f"   • Total Revenue: {format_currency(highest['total_revenue'])}")
print(f"   • Taxpayer Income: {format_currency(highest['taxpayer_income'])}")
print(f"   • Business Revenue: {format_currency(highest['business_revenue'])}")
print(f"   • Property Value: {format_currency(highest['property_value'])}")
print(f"   • Taxpayers: {highest['taxpayer_count']:,.0f}")
print(f"   • Businesses: {highest['business_count']:,.0f}")
print(f"   • Properties: {highest['property_count']:,.0f}")

print(f"\n📉 LOWEST PERFORMING LGA: {lowest['lga_name']}")
print(f"   • Total Revenue: {format_currency(lowest['total_revenue'])}")
print(f"   • Taxpayer Income: {format_currency(lowest['taxpayer_income'])}")
print(f"   • Business Revenue: {format_currency(lowest['business_revenue'])}")
print(f"   • Property Value: {format_currency(lowest['property_value'])}")
print(f"   • Taxpayers: {lowest['taxpayer_count']:,.0f}")
print(f"   • Businesses: {lowest['business_count']:,.0f}")
print(f"   • Properties: {lowest['property_count']:,.0f}")

# Calculate gap
revenue_gap = highest['total_revenue'] - lowest['total_revenue']
gap_percentage = (revenue_gap / highest['total_revenue']) * 100 if highest['total_revenue'] > 0 else 0

print(f"\n📊 REVENUE GAP: {format_currency(revenue_gap)} ({gap_percentage:.1f}% difference)")

# =========================================================
# 6️⃣ Revenue by Business Sector
# =========================================================
print("\n" + "="*60)
print("🏭 BUSINESS SECTOR ANALYSIS")
print("="*60)

# Analyze businesses by sector
sector_summary = businesses.groupby("sector").agg({
    "annual_revenue": "sum",
    "business_name": "count",
    "employee_count": "sum",
    "registered": "sum"
}).reset_index()

sector_summary.rename(columns={
    "annual_revenue": "total_revenue",
    "business_name": "business_count",
    "employee_count": "total_employees",
    "registered": "registered_count"
}, inplace=True)

sector_summary["avg_revenue_per_business"] = sector_summary["total_revenue"] / sector_summary["business_count"]
sector_summary["registration_rate"] = (sector_summary["registered_count"] / sector_summary["business_count"]) * 100
sector_summary = sector_summary.sort_values("total_revenue", ascending=False)

# Display top sectors
print("\n📊 TOP BUSINESS SECTORS BY REVENUE:")
for idx, row in sector_summary.head(5).iterrows():
    print(f"\n  {row['sector']}:")
    print(f"    • Revenue: {format_currency(row['total_revenue'])}")
    print(f"    • Businesses: {row['business_count']:,.0f}")
    print(f"    • Employees: {row['total_employees']:,.0f}")
    print(f"    • Registration Rate: {row['registration_rate']:.1f}%")
    print(f"    • Avg Revenue/Business: {format_currency(row['avg_revenue_per_business'])}")

# Top sector
if not sector_summary.empty:
    top_sector = sector_summary.iloc[0]
    print(f"\n🏆 TOP SECTOR: {top_sector['sector']}")
    print(f"   • Revenue: {format_currency(top_sector['total_revenue'])}")
    print(f"   • Share of Total Business Revenue: {(top_sector['total_revenue'] / total_business_revenue * 100):.1f}%")

# =========================================================
# 7️⃣ Revenue by Occupation (Taxpayers)
# =========================================================
print("\n" + "="*60)
print("👥 OCCUPATION ANALYSIS")
print("="*60)

occupation_summary = taxpayers.groupby("occupation").agg({
    "declared_income": "sum",
    "full_name": "count",
    "compliance_score": "mean",
    "business_owner": "sum"
}).reset_index()

occupation_summary.rename(columns={
    "declared_income": "total_income",
    "full_name": "taxpayer_count",
    "compliance_score": "avg_compliance",
    "business_owner": "business_owners"
}, inplace=True)

occupation_summary["avg_income"] = occupation_summary["total_income"] / occupation_summary["taxpayer_count"]
occupation_summary["business_owner_rate"] = (occupation_summary["business_owners"] / occupation_summary["taxpayer_count"]) * 100
occupation_summary = occupation_summary.sort_values("total_income", ascending=False)

# Display top occupations
print("\n📊 TOP OCCUPATIONS BY INCOME:")
for idx, row in occupation_summary.head(5).iterrows():
    print(f"\n  {row['occupation']}:")
    print(f"    • Total Income: {format_currency(row['total_income'])}")
    print(f"    • Taxpayers: {row['taxpayer_count']:,.0f}")
    print(f"    • Avg Income: {format_currency(row['avg_income'])}")
    print(f"    • Compliance: {row['avg_compliance']:.1f}%")
    print(f"    • Business Owners: {row['business_owner_rate']:.1f}%")

# Top occupation
if not occupation_summary.empty:
    top_occupation = occupation_summary.iloc[0]
    print(f"\n🏆 TOP OCCUPATION: {top_occupation['occupation']}")
    print(f"   • Total Income: {format_currency(top_occupation['total_income'])}")
    print(f"   • Share of Total Taxpayer Income: {(top_occupation['total_income'] / total_taxpayer_income * 100):.1f}%")

# =========================================================
# 8️⃣ Property Analysis
# =========================================================
print("\n" + "="*60)
print("🏠 PROPERTY ANALYSIS")
print("="*60)

property_summary = properties.groupby("property_type").agg({
    "estimated_value": ["sum", "mean", "count"]
}).round(2)

property_summary.columns = ['total_value', 'avg_value', 'property_count']
property_summary = property_summary.reset_index()
property_summary = property_summary.sort_values("total_value", ascending=False)

print("\n📊 PROPERTY TYPES BY VALUE:")
for idx, row in property_summary.iterrows():
    print(f"\n  {row['property_type']}:")
    print(f"    • Total Value: {format_currency(row['total_value'])}")
    print(f"    • Properties: {row['property_count']:,.0f}")
    print(f"    • Avg Value: {format_currency(row['avg_value'])}")

# =========================================================
# 9️⃣ Tax Compliance Analysis
# =========================================================
print("\n" + "="*60)
print("💰 TAX COMPLIANCE ANALYSIS")
print("="*60)

if not tax_records.empty:
    # Merge with taxpayers to get LGA
    tax_with_lga = tax_records.merge(taxpayers[['id', 'lga', 'full_name']], left_on='taxpayer_id', right_on='id')
    
    # Overall compliance
    total_collection_rate = (total_tax_paid / total_tax_expected * 100) if total_tax_expected > 0 else 0
    print(f"\n📊 OVERALL TAX COMPLIANCE:")
    print(f"   • Total Tax Expected: {format_currency(total_tax_expected)}")
    print(f"   • Total Tax Paid: {format_currency(total_tax_paid)}")
    print(f"   • Collection Rate: {total_collection_rate:.1f}%")
    
    # Tax gap
    tax_gap = total_tax_expected - total_tax_paid
    print(f"   • Tax Gap: {format_currency(tax_gap)}")
    
    # Defaulters by LGA
    defaulters = tax_records[tax_records['tax_paid'] < tax_records['expected_tax']]
    if not defaulters.empty:
        defaulters_with_lga = defaulters.merge(taxpayers[['id', 'lga', 'full_name']], left_on='taxpayer_id', right_on='id')
        defaulters_by_lga = defaulters_with_lga.groupby('lga').size().sort_values(ascending=False)
        
        print(f"\n⚠️ TOP LGAs WITH MOST DEFAULTERS:")
        for lga, count in defaulters_by_lga.head(3).items():
            print(f"   • {lga}: {count} defaulters")
        
        # Top defaulters
        top_defaulters = defaulters_with_lga.copy()
        top_defaulters['unpaid'] = top_defaulters['expected_tax'] - top_defaulters['tax_paid']
        top_defaulters = top_defaulters.sort_values('unpaid', ascending=False).head(3)
        
        print(f"\n🚨 TOP DEFAULTERS:")
        for idx, row in top_defaulters.iterrows():
            print(f"   • {row['full_name']} ({row['lga']}): owes {format_currency(row['unpaid'])}")

# =========================================================
# 🔟 Business Registration Analysis
# =========================================================
print("\n" + "="*60)
print("📋 BUSINESS REGISTRATION ANALYSIS")
print("="*60)

if 'registered' in businesses.columns:
    registered_count = businesses['registered'].sum()
    total_businesses = len(businesses)
    registration_rate = (registered_count / total_businesses * 100) if total_businesses > 0 else 0
    
    print(f"\n📊 REGISTRATION STATISTICS:")
    print(f"   • Total Businesses: {total_businesses:,.0f}")
    print(f"   • Registered: {registered_count:,.0f}")
    print(f"   • Unregistered: {total_businesses - registered_count:,.0f}")
    print(f"   • Registration Rate: {registration_rate:.1f}%")
    
    # Registration by sector
    registration_by_sector = businesses.groupby('sector')['registered'].agg(['sum', 'count'])
    registration_by_sector.columns = ['registered', 'total']
    registration_by_sector['rate'] = (registration_by_sector['registered'] / registration_by_sector['total'] * 100)
    registration_by_sector = registration_by_sector.sort_values('rate', ascending=False)
    
    print(f"\n📊 REGISTRATION RATE BY SECTOR:")
    for sector, row in registration_by_sector.head(3).iterrows():
        print(f"   • {sector}: {row['rate']:.1f}% ({row['registered']:.0f}/{row['total']:.0f})")

# =========================================================
# 1️⃣1️⃣ Interactive Map (Enhanced)
# =========================================================
print("\n" + "="*60)
print("🗺️ GENERATING INTERACTIVE MAP")
print("="*60)

# Prepare map data
taxpayers_map = taxpayers.merge(
    lagos_lga[["lga_name", "latitude", "longitude"]],
    left_on="lga",
    right_on="lga_name",
    how="left"
).dropna(subset=["latitude", "longitude"])

# Create map
m = folium.Map(location=[6.5244, 3.3792], zoom_start=10)

# Color coding function
def get_marker_color(row):
    if not occupation_summary.empty and not sector_summary.empty:
        if row["lga"] == highest["lga_name"] and row["occupation"] == top_occupation["occupation"]:
            return "red"  # Top LGA + Top Occupation
        elif row["lga"] == highest["lga_name"]:
            return "green"  # Top LGA
        elif row["occupation"] == top_occupation["occupation"]:
            return "orange"  # Top Occupation
        elif row["lga"] == lowest["lga_name"]:
            return "gray"  # Lowest LGA
        else:
            return "blue"  # Normal
    else:
        return "blue"

# Add markers
for _, row in taxpayers_map.iterrows():
    color = get_marker_color(row)
    
    # Create popup with detailed info
    popup_text = f"""
    <b>{row['full_name']}</b><br>
    <b>LGA:</b> {row['lga']}<br>
    <b>Occupation:</b> {row['occupation']}<br>
    <b>Income:</b> {format_currency(row['declared_income'])}<br>
    <b>Compliance:</b> {row['compliance_score']:.1f}%<br>
    <b>Business Owner:</b> {'Yes' if row['business_owner'] else 'No'}<br>
    <b>Age:</b> {row['age']}
    """
    
    folium.CircleMarker(
        location=[row["latitude"], row["longitude"]],
        radius=6,
        color=color,
        fill=True,
        fill_opacity=0.7,
        popup=folium.Popup(popup_text, max_width=300)
    ).add_to(m)

# Add legend
legend_html = '''
<div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000; background-color: white; padding: 10px; border: 2px solid grey; border-radius: 5px">
    <p><b>Legend</b></p>
    <p><span style="color: red;">●</span> Top LGA + Top Occupation</p>
    <p><span style="color: green;">●</span> Top LGA</p>
    <p><span style="color: orange;">●</span> Top Occupation</p>
    <p><span style="color: gray;">●</span> Lowest LGA</p>
    <p><span style="color: blue;">●</span> Other</p>
</div>
'''
m.get_root().html.add_child(folium.Element(legend_html))

# Save map
map_path = "../data/business_lga_map.html"
m.save(map_path)
print(f"✅ Interactive map saved to {map_path}")

# =========================================================
# 1️⃣2️⃣ Government Recommendations (Enhanced)
# =========================================================
print("\n" + "="*60)
print("🏛️ GOVERNMENT RECOMMENDATIONS")
print("="*60)

print("\n📋 STRATEGIC INSIGHTS:")

# Revenue concentration
top_3_lgas = summary.head(3)['total_revenue'].sum()
top_3_percent = (top_3_lgas / TOTAL_REVENUE) * 100
print(f"   • Top 3 LGAs contribute {top_3_percent:.1f}% of total revenue")

# Sector recommendations
if not sector_summary.empty:
    print(f"\n🏭 SECTOR FOCUS:")
    print(f"   • Prioritize {top_sector['sector']} sector - generates {format_currency(top_sector['total_revenue'])}")
    if top_sector['registration_rate'] < 80:
        print(f"   • Improve registration in {top_sector['sector']} sector ({top_sector['registration_rate']:.1f}% registered)")

# LGA recommendations
print(f"\n📍 LGA SPECIFIC:")
print(f"   • Investigate {lowest['lga_name']} - revenue is {format_currency(lowest['total_revenue'])}")
print(f"   • Study {highest['lga_name']} for best practices - {format_currency(highest['total_revenue'])}")

# Tax compliance recommendations
if not tax_records.empty and total_collection_rate < 80:
    print(f"\n💰 TAX COMPLIANCE:")
    print(f"   • Improve collection rate ({total_collection_rate:.1f}%)")
    print(f"   • Focus on top defaulters - total gap is {format_currency(tax_gap)}")

# Business registration recommendations
if 'registered' in businesses.columns and registration_rate < 70:
    print(f"\n📋 BUSINESS REGISTRATION:")
    print(f"   • Increase registration drive - current rate is {registration_rate:.1f}%")
    print(f"   • Target unregistered businesses in {summary.head(1)['lga_name'].values[0]}")

# Occupation recommendations
if not occupation_summary.empty:
    print(f"\n👥 WORKFORCE DEVELOPMENT:")
    print(f"   • Support {top_occupation['occupation']} professionals - {top_occupation['taxpayer_count']:,.0f} taxpayers")
    print(f"   • Average income in top occupation: {format_currency(top_occupation['avg_income'])}")

print("\n" + "="*60)
print("✅ LAGOS REVENUE INTELLIGENCE ANALYSIS COMPLETE")
print("="*60)





































































# import os
# import pandas as pd
# from sqlalchemy import create_engine
# import folium
# from branca.element import Template, MacroElement

# # =========================================================
# # Display large numbers nicely
# # =========================================================
# pd.set_option('display.float_format', '{:,.0f}'.format)

# # =========================================================
# # 0️⃣ Database Connection
# # =========================================================
# DB_USER = os.getenv("DB_USER", "iwebukeonyeamachi")
# DB_PASS = os.getenv("DB_PASS", "Richiwin")
# DB_HOST = os.getenv("DB_HOST", "localhost")
# DB_NAME = os.getenv("DB_NAME", "lagos_revenue")

# engine = create_engine(
#     f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
# )

# print(" Connected to Lagos Revenue Database")

# # =========================================================
# # 1️⃣ Load LGAs
# # =========================================================
# current_dir = os.path.dirname(os.path.abspath(__file__))
# csv_path = os.path.join(current_dir, "../data/lagos_lgas.csv")

# lagos_lga = pd.read_csv(csv_path)

# # =========================================================
# # 2️⃣ Load Data
# # =========================================================
# taxpayers = pd.read_sql("SELECT * FROM taxpayers", engine)
# businesses = pd.read_sql("SELECT * FROM businesses", engine)
# properties = pd.read_sql("SELECT * FROM properties", engine)

# # =========================================================
# # 3️⃣ GLOBAL REVENUE (SOURCE OF TRUTH)
# # =========================================================
# total_taxpayer_income = taxpayers["declared_income"].sum()
# total_business_income = businesses["annual_revenue"].sum()
# total_property_income = properties["estimated_value"].sum()

# TOTAL_REVENUE = (
#     total_taxpayer_income +
#     total_business_income +
#     total_property_income
# )

# print(f"\n TOTAL SYSTEM REVENUE: ₦{TOTAL_REVENUE:,.2f}")

# # =========================================================
# # 4️⃣ Revenue by LGA
# # =========================================================
# taxpayer_income = taxpayers.groupby("lga")["declared_income"].sum().reset_index()
# taxpayer_income.rename(columns={"declared_income": "taxpayer_income"}, inplace=True)

# business_income = businesses.groupby("lga")["annual_revenue"].sum().reset_index()
# business_income.rename(columns={"annual_revenue": "business_income"}, inplace=True)

# property_income = properties.groupby("lga")["estimated_value"].sum().reset_index()
# property_income.rename(columns={"estimated_value": "property_income"}, inplace=True)

# summary = lagos_lga.merge(
#     taxpayer_income, left_on="lga_name", right_on="lga", how="left"
# ).merge(
#     business_income, left_on="lga_name", right_on="lga", how="left"
# ).merge(
#     property_income, left_on="lga_name", right_on="lga", how="left"
# )

# summary = summary.fillna(0)

# summary["total_income"] = (
#     summary["taxpayer_income"] +
#     summary["business_income"] +
#     summary["property_income"]
# )

# # =========================================================
# # 5️⃣ Highest and Lowest LGA
# # =========================================================
# highest = summary.loc[summary["total_income"].idxmax()]
# lowest = summary.loc[summary["total_income"].idxmin()]

# print("\n LGA Revenue Summary:")
# print(summary[["lga_name", "total_income"]])

# print(f"\n Highest Paying LGA: {highest['lga_name']} (₦{highest['total_income']:,.2f})")
# print(f"⚠️ Lowest Paying LGA: {lowest['lga_name']} (₦{lowest['total_income']:,.2f})")

# # =========================================================
# # 6️⃣ Revenue by Business Occupation
# # =========================================================
# business_summary = (
#     taxpayers.groupby("occupation")["declared_income"]
#     .sum()
#     .reset_index()
#     .rename(columns={"declared_income": "total_income"})
# )

# business_summary = business_summary.sort_values(by="total_income", ascending=False)

# top_business = business_summary.iloc[0]

# print(f"\n Top Revenue Business Type: {top_business['occupation']} (₦{top_business['total_income']:,.2f})")

# # =========================================================
# # 7️⃣ Interactive Map
# # =========================================================
# taxpayers_map = taxpayers.merge(
#     lagos_lga[["lga_name", "latitude", "longitude"]],
#     left_on="lga",
#     right_on="lga_name",
#     how="left"
# ).dropna(subset=["latitude", "longitude"])

# m = folium.Map(location=[6.5244, 3.3792], zoom_start=10)

# for _, row in taxpayers_map.iterrows():

#     color = "blue"

#     if row["lga"] == highest["lga_name"]:
#         color = "green"

#     if row["occupation"] == top_business["occupation"]:
#         color = "orange"

#     if row["lga"] == highest["lga_name"] and row["occupation"] == top_business["occupation"]:
#         color = "red"

#     folium.CircleMarker(
#         location=[row["latitude"], row["longitude"]],
#         radius=6,
#         color=color,
#         fill=True,
#         fill_opacity=0.7,
#         popup=f"{row['full_name']} | ₦{row['declared_income']:,.0f} | {row['occupation']} | {row['lga']}"
#     ).add_to(m)

# # Save map
# m.save("../data/business_lga_map.html")

# print("\n Interactive map saved")

# # =========================================================
# # 8️⃣ Government Recommendations
# # =========================================================
# print("\n Government Recommendations:")
# print(f"- Focus policy incentives on {top_business['occupation']} sector.")
# print(f"- Investigate low-performing LGA: {lowest['lga_name']}.")
# print(f"- Strengthen compliance in high-performing LGA: {highest['lga_name']}.")

# print("\n Lagos Revenue Intelligence Analysis Complete.")




































# import os
# import pandas as pd
# from sqlalchemy import create_engine
# import folium
# from branca.element import Template, MacroElement
# import numpy as np

# # =========================================================
# # Display large numbers nicely
# # =========================================================
# pd.set_option('display.float_format', '{:,.0f}'.format)

# # =========================================================
# # 0️⃣ Database Connection
# # =========================================================
# DB_USER = os.getenv("DB_USER", "iwebukeonyeamachi")
# DB_PASS = os.getenv("DB_PASS", "Richiwin")
# DB_HOST = os.getenv("DB_HOST", "localhost")
# DB_NAME = os.getenv("DB_NAME", "lagos_revenue")

# engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")
# print("✅ Connected to Lagos Revenue Database")

# # =========================================================
# # 1️⃣ Load LGAs from CSV (for local govt matching)
# # =========================================================
# try:
#     current_dir = os.path.dirname(os.path.abspath(__file__))
#     csv_path = os.path.join(current_dir, "../data/lagos_lgas.csv")

#     lagos_lga = pd.read_csv(csv_path)
#     LGA_LIST = [lga.strip().lower() for lga in lagos_lga["lga_name"].tolist()]
#     print("✅ Loaded LGAs successfully from CSV")

# except Exception as e:
#     print("⚠️ Error loading lagos_lgas.csv:", e)
#     lagos_lga = pd.DataFrame(columns=['lga_name','latitude','longitude'])
#     LGA_LIST = []

# # =========================================================
# # 2️⃣ Load Taxpayers, Businesses, Properties
# # =========================================================
# taxpayers = pd.read_sql("SELECT * FROM taxpayers", engine)
# businesses = pd.read_sql("SELECT * FROM businesses", engine)
# properties = pd.read_sql("SELECT * FROM properties", engine)

# # =========================================================
# # 3️⃣ Compute Total Income by LGA
# # =========================================================
# taxpayer_income = taxpayers.groupby('lga')['declared_income'].sum().reset_index().rename(columns={'declared_income':'taxpayer_income'})
# business_income = businesses.groupby('lga')['annual_revenue'].sum().reset_index().rename(columns={'annual_revenue':'business_income'})
# property_income = properties.groupby('lga')['estimated_value'].sum().reset_index().rename(columns={'estimated_value':'property_income'})

# summary = lagos_lga[['lga_name','latitude','longitude']].merge(
#     taxpayer_income, left_on='lga_name', right_on='lga', how='left'
# ).merge(
#     business_income, left_on='lga_name', right_on='lga', how='left'
# ).merge(
#     property_income, left_on='lga_name', right_on='lga', how='left'
# )

# summary[['taxpayer_income','business_income','property_income']] = summary[['taxpayer_income','business_income','property_income']].fillna(0)
# summary['total_income'] = summary['taxpayer_income'] + summary['business_income'] + summary['property_income']

# # =========================================================
# # 4️⃣ Summaries
# # =========================================================
# highest = summary.loc[summary['total_income'].idxmax()]
# lowest = summary.loc[summary['total_income'].idxmin()]

# print("\n📊 LGA Revenue Summary:")
# print(summary[['lga_name','total_income']])
# print(f"\n🏆 Highest Paying LGA: {highest['lga_name']} ({highest['total_income']:,} NGN)")
# print(f"⚠️ Lowest Paying LGA: {lowest['lga_name']} ({lowest['total_income']:,} NGN)")

# # =========================================================
# # 5️⃣ Revenue by Occupation
# # =========================================================
# business_summary = taxpayers.groupby('occupation')['declared_income'].sum().reset_index().rename(columns={'declared_income':'total_income'})
# business_summary = business_summary.sort_values(by='total_income', ascending=False)
# growth_rate = 0.15
# business_summary['projected_next_year'] = business_summary['total_income'] * (1 + growth_rate)

# top_business = business_summary.iloc[0]
# print(f"\n🧠 Top Revenue Business Type: {top_business['occupation']} ({top_business['total_income']:,} NGN)")

# # =========================================================
# # 6️⃣ Interactive Map
# # =========================================================
# taxpayers_map = taxpayers.merge(
#     lagos_lga[['lga_name','latitude','longitude']],
#     left_on='lga',
#     right_on='lga_name',
#     how='left'
# ).dropna(subset=['latitude','longitude'])

# m = folium.Map(location=[6.5244,3.3792], zoom_start=10)
# for _, row in taxpayers_map.iterrows():
#     color = "blue"
#     if row['lga'] == highest['lga_name']:
#         color = "green"
#     if row['occupation'] == top_business['occupation']:
#         color = "orange"
#     if row['lga'] == highest['lga_name'] and row['occupation'] == top_business['occupation']:
#         color = "red"

#     folium.CircleMarker(
#         location=[row['latitude'], row['longitude']],
#         radius=6,
#         color=color,
#         fill=True,
#         fill_opacity=0.7,
#         popup=f"{row['full_name']} | Income: ₦{row['declared_income']:,} | {row['occupation']} | LGA: {row['lga']}"
#     ).add_to(m)

# # LGA markers
# for _, row in lagos_lga.iterrows():
#     if pd.notna(row['latitude']) and pd.notna(row['longitude']):
#         folium.Marker(
#             location=[row['latitude'], row['longitude']],
#             icon=folium.Icon(color='red'),
#             popup=row['lga_name']
#         ).add_to(m)

# # Legend
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
# print("\n🗺️ Interactive map saved: business_lga_map.html")

# # =========================================================
# # 7️⃣ Government Recommendations
# # =========================================================
# print("\n🏛 Government Recommendations:")
# print(f"- Focus policy incentives on {top_business['occupation']} sector.")
# print(f"- Investigate low-performing LGA: {lowest['lga_name']}.")
# print(f"- Strengthen compliance in high-performing LGA: {highest['lga_name']}.")
# print("\n🚀 Lagos Revenue Intelligence Analysis Complete.")


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


