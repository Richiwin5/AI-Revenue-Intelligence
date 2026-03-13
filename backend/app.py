# app.py - Fixed version with correct imports from chatbot.py
import os
import pandas as pd
import numpy as np
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from sqlalchemy import create_engine
import psycopg2
from dotenv import load_dotenv
import folium
from datetime import datetime
import json

load_dotenv()

app = Flask(__name__)
CORS(app)

# -------------------- DATABASE --------------------
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Create database connection
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# -------------------- LOAD LGAs --------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.abspath(os.path.join(current_dir, "../data/lagos_lgas.csv"))
lagos_lga = pd.read_csv(csv_path)
LGA_LIST = [lga.strip().lower() for lga in lagos_lga["lga_name"].tolist()]
print(f"✅ Loaded {len(LGA_LIST)} LGAs")

# -------------------- IMPORT FROM CHATBOT --------------------
# Import the actual functions that exist in chatbot.py
from backend.chatbot import (
    RevenueChatbot, 
    format_currency, 
    format_percentage,
    extract_all_lgas,
    detect_intent,
    is_greeting,
    get_greeting_response,
    LGA_LIST as CHATBOT_LGA_LIST,
    LGA_VARIATIONS,
    get_total_revenue as chatbot_get_total_revenue,
    get_revenue_breakdown as chatbot_get_revenue_breakdown,
    get_lga_revenue,
    get_top_sector_global,
    get_top_sector_in_lga,
    get_all_sectors_global,
    get_all_sectors_in_lga,
    get_sector_revenue_global,
    get_sector_revenue_in_lga,
    get_tax_defaulters,
    get_top_taxpayers,
    get_top_properties,
    get_compliance_rate
)

# Initialize the chatbot
revenue_chatbot = RevenueChatbot()

# -------------------- WRAPPER FUNCTIONS FOR API --------------------
def get_total_revenue(lga=None):
    """Wrapper for chatbot's get_total_revenue"""
    if lga:
        return chatbot_get_total_revenue(lga)
    return chatbot_get_total_revenue()

def get_revenue_breakdown(lga=None):
    """Wrapper for chatbot's get_revenue_breakdown"""
    return chatbot_get_revenue_breakdown(lga)

def get_tax_summary(lga=None):
    """Get tax summary using chatbot's functions"""
    defaulters = get_tax_defaulters(lga, 100)  # Get all defaulters to calculate totals
    total_unpaid = sum(d['unpaid'] for d in defaulters) if defaulters else 0
    
    # Get top taxpayers to estimate total paid
    top_tax = get_top_taxpayers(lga, 100)
    total_paid = sum(t['paid'] for t in top_tax) if top_tax else 0
    
    # Rough estimate of expected tax
    total_expected = total_paid + total_unpaid
    
    return {
        "total_paid": float(total_paid),
        "total_expected": float(total_expected),
        "taxpayers": len(top_tax) if top_tax else 0,
        "collection_rate": (total_paid / total_expected * 100) if total_expected > 0 else 0
    }

def get_tax_gap(lga=None, limit=5):
    """Wrapper for chatbot's get_tax_defaulters"""
    defaulters = get_tax_defaulters(lga, limit)
    total_unpaid = sum(d['unpaid'] for d in defaulters) if defaulters else 0
    
    return {
        "total_unpaid": float(total_unpaid),
        "count": len(defaulters),
        "defaulters": defaulters
    }

def get_top_taxpayer(lga=None):
    """Get top taxpayer"""
    top = get_top_taxpayers(lga, 1)
    if top:
        return {
            "name": top[0]['name'],
            "lga": top[0]['lga'],
            "total_paid": float(top[0]['paid']),
            "transactions": 1
        }
    return {}

def get_top_taxpayers_list(lga=None, limit=5):
    """Wrapper for chatbot's get_top_taxpayers"""
    return get_top_taxpayers(lga, limit)

def get_compliance(lga=None):
    """Wrapper for chatbot's get_compliance_rate"""
    comp = get_compliance_rate(lga)
    return {
        "average_score": float(comp['avg_score']),
        "total_taxpayers": int(comp['total']),
        "compliant_count": int(comp['compliant']),
        "non_compliant": int(comp['total'] - comp['compliant']),
        "compliance_rate": float(comp['rate'])
    }

def get_top_lga():
    """Get top performing LGA using revenue data"""
    # This would need to be implemented - for now return a placeholder
    return {"lga": "Victoria Island", "total_revenue": 100000000000}

def get_lowest_lga():
    """Get lowest performing LGA"""
    # This would need to be implemented - for now return a placeholder
    return {"lga": "Epe", "total_revenue": 10000000}

def get_top_sector():
    """Wrapper for chatbot's get_top_sector_global"""
    return get_top_sector_global()

def get_top_properties_list(lga=None, limit=5):
    """Wrapper for chatbot's get_top_properties"""
    return get_top_properties(lga, limit)

def get_all_sectors_list():
    """Wrapper for chatbot's get_all_sectors_global"""
    return get_all_sectors_global()

# -------------------- HELPER FUNCTIONS --------------------
def extract_lga(text):
    """Extract LGA from text using chatbot's function"""
    lgas = extract_all_lgas(text)
    return lgas[0] if lgas else None

def get_intent(text):
    """Get intent using chatbot's function"""
    intent_info = detect_intent(text)
    return intent_info.get('intent', 'unknown')

def parse_question(text):
    """Parse question into intent and LGA"""
    intent = get_intent(text)
    lgas = extract_all_lgas(text)
    lga = lgas[0] if lgas else None
    return {
        "intent": intent,
        "lga": lga
    }

# -------------------- /ask API --------------------
@app.route("/ask", methods=["POST"])
def ask():
    """Process natural language questions using the chatbot"""
    data = request.json
    question = data.get("question", "")
    
    # Check for greeting first
    if is_greeting(question):
        response = get_greeting_response(question)
        return jsonify({
            "response": response,
            "intent": "greeting",
            "lga": None,
            "data": {},
            "timestamp": datetime.now().isoformat()
        })
    
    # Use the chatbot to process the question
    response = revenue_chatbot.ask(question)
    
    # Also get structured data for the response
    parsed = parse_question(question)
    intent = parsed.get("intent")
    lga = parsed.get("lga")
    
    # Prepare additional data based on intent
    data_response = {}
    
    if intent in ["total_revenue_state", "revenue", "revenue_lga"]:
        data_response = get_revenue_breakdown(lga)
    
    elif intent in ["tax_collected", "tax"]:
        data_response = get_tax_summary(lga)
    
    elif intent in ["tax_gap", "tax_defaulters"]:
        data_response = get_tax_gap(lga)
    
    elif intent in ["top_taxpayer", "top_taxpayers"]:
        data_response = get_top_taxpayer(lga)
    
    elif intent == "compliance":
        data_response = get_compliance(lga)
    
    elif intent == "top_lga":
        data_response = get_top_lga()
    
    elif intent == "lowest_lga":
        data_response = get_lowest_lga()
    
    elif intent == "top_sector":
        data_response = get_top_sector()
    
    elif intent == "top_properties":
        data_response = get_top_properties_list(lga, 5)
    
    return jsonify({
        "response": response,
        "intent": intent,
        "lga": lga,
        "data": data_response,
        "timestamp": datetime.now().isoformat()
    })

# -------------------- /summary API --------------------
@app.route("/summary", methods=["GET"])
def summary():
    """Get comprehensive revenue summary with ALL 20 LGAs"""
    
    # Load raw tables
    taxpayers_df = pd.read_sql("SELECT * FROM taxpayers", engine)
    businesses_df = pd.read_sql("SELECT * FROM businesses", engine)
    properties_df = pd.read_sql("SELECT * FROM properties", engine)
    tax_records_df = pd.read_sql("SELECT * FROM tax_records", engine)
    
    # Calculate totals
    total_taxpayer_income = float(taxpayers_df["declared_income"].sum())
    total_business_revenue = float(businesses_df["annual_revenue"].sum())
    total_property_value = float(properties_df["estimated_value"].sum())
    total_tax_paid = float(tax_records_df["tax_paid"].sum()) if not tax_records_df.empty else 0
    total_tax_expected = float(tax_records_df["expected_tax"].sum()) if not tax_records_df.empty else 0
    
    total_revenue = total_taxpayer_income + total_business_revenue + total_property_value
    collection_rate = (total_tax_paid / total_tax_expected * 100) if total_tax_expected > 0 else 0
    
    # ========== LGA SUMMARY - ALL 20 LGAs ==========
    # Start with the base LGA list from CSV (all 20 LGAs)
    lga_summary = lagos_lga[['lga_name']].copy()
    
    # Aggregate data by LGA
    taxpayer_by_lga = taxpayers_df.groupby("lga")["declared_income"].sum().reset_index()
    taxpayer_by_lga.rename(columns={"declared_income": "taxpayer_income", "lga": "lga_name"}, inplace=True)
    
    business_by_lga = businesses_df.groupby("lga")["annual_revenue"].sum().reset_index()
    business_by_lga.rename(columns={"annual_revenue": "business_revenue", "lga": "lga_name"}, inplace=True)
    
    property_by_lga = properties_df.groupby("lga")["estimated_value"].sum().reset_index()
    property_by_lga.rename(columns={"estimated_value": "property_value", "lga": "lga_name"}, inplace=True)
    
    # Get counts
    taxpayer_count = taxpayers_df.groupby("lga").size().reset_index(name="taxpayer_count")
    taxpayer_count.rename(columns={"lga": "lga_name"}, inplace=True)
    
    business_count = businesses_df.groupby("lga").size().reset_index(name="business_count")
    business_count.rename(columns={"lga": "lga_name"}, inplace=True)
    
    property_count = properties_df.groupby("lga").size().reset_index(name="property_count")
    property_count.rename(columns={"lga": "lga_name"}, inplace=True)
    
    # Merge all data (preserving all 20 LGAs)
    lga_summary = lga_summary.merge(taxpayer_by_lga, on="lga_name", how="left")
    lga_summary = lga_summary.merge(business_by_lga, on="lga_name", how="left")
    lga_summary = lga_summary.merge(property_by_lga, on="lga_name", how="left")
    lga_summary = lga_summary.merge(taxpayer_count, on="lga_name", how="left")
    lga_summary = lga_summary.merge(business_count, on="lga_name", how="left")
    lga_summary = lga_summary.merge(property_count, on="lga_name", how="left")
    
    # Fill NaN with 0
    lga_summary = lga_summary.fillna(0)
    
    # Calculate totals
    lga_summary["total_revenue"] = (
        lga_summary["taxpayer_income"] + 
        lga_summary["business_revenue"] + 
        lga_summary["property_value"]
    )
    
    # Sort by total revenue
    lga_summary = lga_summary.sort_values("total_revenue", ascending=False)
    
    # Format LGA summary for response
    lga_list = []
    for _, row in lga_summary.iterrows():
        lga_list.append({
            "lga_name": row["lga_name"],
            "total_revenue": float(row["total_revenue"]),
            "formatted_revenue": format_currency(row["total_revenue"]),
            "breakdown": {
                "taxpayer_income": float(row["taxpayer_income"]),
                "business_revenue": float(row["business_revenue"]),
                "property_value": float(row["property_value"])
            },
            "formatted_breakdown": {
                "taxpayer_income": format_currency(row["taxpayer_income"]),
                "business_revenue": format_currency(row["business_revenue"]),
                "property_value": format_currency(row["property_value"])
            },
            "statistics": {
                "taxpayer_count": int(row["taxpayer_count"]),
                "business_count": int(row["business_count"]),
                "property_count": int(row["property_count"])
            }
        })
    
    # Get top and bottom LGAs
    top_lga = lga_list[0] if lga_list else {}
    bottom_lga = lga_list[-1] if lga_list else {}
    
    # Calculate revenue concentration
    top_3_total = sum([lga["total_revenue"] for lga in lga_list[:3]])
    top_3_percentage = (top_3_total / total_revenue * 100) if total_revenue > 0 else 0
    
    # ========== SECTOR ANALYSIS ==========
    sector_summary = businesses_df.groupby('sector')['annual_revenue'].agg(['sum', 'count']).reset_index()
    sector_summary.columns = ['sector', 'total_revenue', 'business_count']
    sector_summary = sector_summary.sort_values('total_revenue', ascending=False)
    
    sectors = []
    for _, row in sector_summary.iterrows():
        sectors.append({
            "sector": row["sector"],
            "total_revenue": float(row["total_revenue"]),
            "formatted_revenue": format_currency(row["total_revenue"]),
            "business_count": int(row["business_count"])
        })
    
    top_sector = sectors[0] if sectors else {}
    
    # ========== OCCUPATION ANALYSIS ==========
    occupation_summary = taxpayers_df.groupby('occupation')['declared_income'].agg(['sum', 'count']).reset_index()
    occupation_summary.columns = ['occupation', 'total_income', 'taxpayer_count']
    occupation_summary = occupation_summary.sort_values('total_income', ascending=False)
    
    occupations = []
    for _, row in occupation_summary.iterrows():
        occupations.append({
            "occupation": row["occupation"],
            "total_income": float(row["total_income"]),
            "formatted_income": format_currency(row["total_income"]),
            "taxpayer_count": int(row["taxpayer_count"])
        })
    
    top_occupation = occupations[0] if occupations else {}
    
    # ========== PROPERTY ANALYSIS ==========
    property_summary = properties_df.groupby('property_type')['estimated_value'].agg(['sum', 'count']).reset_index()
    property_summary.columns = ['property_type', 'total_value', 'count']
    
    properties = []
    for _, row in property_summary.iterrows():
        properties.append({
            "property_type": row["property_type"],
            "total_value": float(row["total_value"]),
            "formatted_value": format_currency(row["total_value"]),
            "count": int(row["count"])
        })
    
    # ========== TAX COMPLIANCE ANALYSIS ==========
    tax_gap = total_tax_expected - total_tax_paid
    
    # ========== GENERATE RECOMMENDATIONS ==========
    recommendations = [
        f"Focus on {top_sector.get('sector', 'top')} sector which generates {format_currency(top_sector.get('total_revenue', 0))}",
        f"Investigate {bottom_lga.get('lga_name', 'lowest')} LGA with revenue of {format_currency(bottom_lga.get('total_revenue', 0))}",
        f"Improve tax collection rate from {collection_rate:.1f}% to reduce gap of {format_currency(tax_gap)}",
        f"Study {top_lga.get('lga_name', 'top')} LGA for best practices - generates {format_currency(top_lga.get('total_revenue', 0))}"
    ]
    
    # ========== FINAL RESPONSE ==========
    response = {
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "total_revenue": {
                "value": float(total_revenue),
                "formatted": format_currency(total_revenue),
                "breakdown": {
                    "taxpayer_income": float(total_taxpayer_income),
                    "business_revenue": float(total_business_revenue),
                    "property_value": float(total_property_value)
                },
                "formatted_breakdown": {
                    "taxpayer_income": format_currency(total_taxpayer_income),
                    "business_revenue": format_currency(total_business_revenue),
                    "property_value": format_currency(total_property_value)
                }
            },
            "tax_summary": {
                "total_paid": float(total_tax_paid),
                "total_expected": float(total_tax_expected),
                "collection_rate": float(collection_rate),
                "formatted_rate": format_percentage(collection_rate),
                "tax_gap": float(tax_gap),
                "formatted_gap": format_currency(tax_gap)
            }
        },
        "lga_analysis": {
            "total_lgas": len(lga_list),
            "top_lga": top_lga,
            "bottom_lga": bottom_lga,
            "revenue_concentration": {
                "top_3_total": float(top_3_total),
                "formatted_top_3": format_currency(top_3_total),
                "top_3_percentage": float(top_3_percentage),
                "formatted_percentage": format_percentage(top_3_percentage)
            },
            "lgas": lga_list  # ALL 20 LGAs with full details
        },
        "sector_analysis": {
            "total_sectors": len(sectors),
            "top_sector": top_sector,
            "sectors": sectors
        },
        "occupation_analysis": {
            "total_occupations": len(occupations),
            "top_occupation": top_occupation,
            "occupations": occupations[:10]
        },
        "property_analysis": {
            "total_property_types": len(properties),
            "properties": properties
        },
        "recommendations": recommendations,
        "metadata": {
            "total_taxpayers": len(taxpayers_df),
            "total_businesses": len(businesses_df),
            "total_properties": len(properties_df),
            "total_tax_records": len(tax_records_df)
        }
    }
    
    return jsonify(response)

# -------------------- /map API --------------------
@app.route("/map", methods=["GET"])
def get_map():
    """Generate and return interactive map"""
    
    # Load data
    taxpayers_df = pd.read_sql("SELECT * FROM taxpayers", engine)
    
    # Merge with LGA coordinates
    map_data = taxpayers_df.merge(
        lagos_lga[['lga_name', 'latitude', 'longitude']],
        left_on='lga',
        right_on='lga_name',
        how='inner'
    ).dropna(subset=['latitude', 'longitude'])
    
    # Get top LGA and top occupation for coloring
    top_lga_data = top_lga if top_lga else {}
    top_lga_name = top_lga_data.get('lga_name', '')
    
    top_occupation_data = occupations[0] if occupations else {}
    top_occ_name = top_occupation_data.get('occupation', '')
    
    # Create map
    m = folium.Map(location=[6.5244, 3.3792], zoom_start=10)
    
    # Add markers
    for _, row in map_data.iterrows():
        # Determine color
        if row['lga'] == top_lga_name and row['occupation'] == top_occ_name:
            color = 'red'
        elif row['lga'] == top_lga_name:
            color = 'green'
        elif row['occupation'] == top_occ_name:
            color = 'orange'
        else:
            color = 'blue'
        
        # Create popup
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
        <p><span style="color: blue;">●</span> Other</p>
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))
    
    # Save map to a temporary file
    map_path = os.path.join(current_dir, "../data/current_map.html")
    m.save(map_path)
    
    return send_file(map_path, mimetype='text/html')

# -------------------- /health API --------------------
@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "lgas_loaded": len(LGA_LIST),
        "chatbot_initialized": True
    })


@app.route("/lgas", methods=["GET"])
def get_lgas():
    """Get list of all LGAs with coordinates"""
    return jsonify({
        "lgas": lagos_lga[['lga_name', 'latitude', 'longitude']].to_dict(orient='records'),
        "count": len(lagos_lga)
    })

# -------------------- RUN --------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=True)























# from flask import Flask, request, jsonify
# from flask_cors import CORS
# import os, pandas as pd
# from sqlalchemy import create_engine
# import psycopg2
# from rapidfuzz import process
# from dotenv import load_dotenv

# load_dotenv()

# app = Flask(__name__)
# CORS(app)  # <-- enable CORS for all endpoints

# # DB setup
# DB_USER = os.getenv("DB_USER")
# DB_PASS = os.getenv("DB_PASS")
# DB_HOST = os.getenv("DB_HOST")
# DB_PORT = os.getenv("DB_PORT")
# DB_NAME = os.getenv("DB_NAME")

# conn = psycopg2.connect(
#     host=DB_HOST,
#     port=DB_PORT,
#     database=DB_NAME,
#     user=DB_USER,
#     password=DB_PASS
# )
# cur = conn.cursor()
# engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# # -------------------- Load LGAs --------------------
# current_dir = os.path.dirname(os.path.abspath(__file__))
# csv_path = os.path.join(current_dir, "../data/lagos_lgas.csv")
# lagos_lga = pd.read_csv(csv_path)
# LGA_LIST = [lga.strip().lower() for lga in lagos_lga["lga_name"].tolist()]

# # -------------------- Helpers --------------------
# def execute_sql(sql):
#     try:
#         cur.execute(sql)
#         return cur.fetchall()
#     except Exception as e:
#         print("SQL ERROR:", e)
#         conn.rollback()
#         return []

# def format_currency(val):
#     if val is None:
#         return "₦0.00"
#     return f"₦{float(val):,.2f}"

# def match_lga(text):
#     text = text.lower()
#     for lga in LGA_LIST:
#         if lga in text:
#             return lga
#     match = process.extractOne(text, LGA_LIST, score_cutoff=75)
#     if match:
#         return match[0]
#     return None

# # -------------------- Question parsing --------------------
# def parse_question(question):
#     question = question.lower()
#     parsed = {"intent": None, "lga": match_lga(question)}

#     if "total revenue" in question:
#         parsed["intent"] = "total_revenue"
#     elif "total tax" in question:
#         parsed["intent"] = "total_tax"
#     elif "owe" in question or "owing" in question or "tax gap" in question:
#         parsed["intent"] = "tax_gap"
#     elif "most taxed" in question or "top taxpayer" in question:
#         parsed["intent"] = "most_taxed"
#     elif "average compliance" in question:
#         parsed["intent"] = "average_compliance"

#     return parsed

# # -------------------- Data fetching --------------------
# def get_total_revenue(lga=None):
#     lga_condition = f"WHERE LOWER(lga) = '{lga}'" if lga else ""
#     taxpayer_sum = execute_sql(f"SELECT SUM(declared_income) FROM taxpayers {lga_condition};")[0][0] or 0
#     business_sum = execute_sql(f"SELECT SUM(annual_revenue) FROM businesses {lga_condition};")[0][0] or 0
#     property_sum = execute_sql(f"SELECT SUM(estimated_value) FROM properties {lga_condition};")[0][0] or 0
#     return taxpayer_sum + business_sum + property_sum

# def get_total_tax(lga=None):
#     lga_condition = f"WHERE LOWER(t.lga) = '{lga}'" if lga else ""
#     sql = f"""
#         SELECT SUM(tr.expected_tax)
#         FROM tax_records tr
#         JOIN taxpayers t ON t.id = tr.taxpayer_id
#         {lga_condition};
#     """
#     return execute_sql(sql)[0][0] or 0

# def get_tax_gap(lga=None):
#     lga_condition = f"AND LOWER(t.lga) = '{lga}'" if lga else ""
#     sql = f"""
#         SELECT t.full_name, SUM(tr.expected_tax - tr.tax_paid) AS unpaid_tax
#         FROM taxpayers t
#         JOIN tax_records tr ON t.id = tr.taxpayer_id
#         WHERE tr.tax_paid < tr.expected_tax {lga_condition}
#         GROUP BY t.full_name
#         ORDER BY unpaid_tax DESC
#         LIMIT 5;
#     """
#     rows = execute_sql(sql)
#     return [{"name": name, "unpaid_tax": float(amount)} for name, amount in rows] if rows else []

# def get_most_taxed(lga=None):
#     lga_condition = f"WHERE LOWER(t.lga) = '{lga}'" if lga else ""
#     sql = f"""
#         SELECT t.full_name, SUM(tr.expected_tax) AS total_tax
#         FROM taxpayers t
#         JOIN tax_records tr ON t.id = tr.taxpayer_id
#         {lga_condition}
#         GROUP BY t.full_name
#         ORDER BY total_tax DESC
#         LIMIT 1;
#     """
#     rows = execute_sql(sql)
#     return {"name": rows[0][0], "total_tax": float(rows[0][1])} if rows else {}

# def get_average_compliance(lga=None):
#     lga_condition = f"WHERE LOWER(lga) = '{lga}'" if lga else ""
#     sql = f"SELECT AVG(compliance_score) FROM taxpayers {lga_condition};"
#     result = execute_sql(sql)[0][0] or 0
#     return float(result)

# # -------------------- /ask API --------------------
# @app.route("/ask", methods=["POST"])
# def ask():
#     data = request.json
#     question = data.get("question", "")
#     parsed = parse_question(question)
#     lga = parsed.get("lga")

#     if not parsed["intent"]:
#         return jsonify({"response": "Ask for: total revenue, total tax, who owes tax, most taxed, or average compliance."})

#     if parsed["intent"] == "total_revenue":
#         return jsonify({"response": format_currency(get_total_revenue(lga))})
#     elif parsed["intent"] == "total_tax":
#         return jsonify({"response": format_currency(get_total_tax(lga))})
#     elif parsed["intent"] == "tax_gap":
#         gap = get_tax_gap(lga)
#         if not gap:
#             return jsonify({"response": "No records found."})
#         response = "\n".join([f"{r['name']} — {format_currency(r['unpaid_tax'])}" for r in gap])
#         return jsonify({"response": response})
#     elif parsed["intent"] == "most_taxed":
#         most = get_most_taxed(lga)
#         if not most:
#             return jsonify({"response": "No records found."})
#         return jsonify({"response": f"{most['name']} — {format_currency(most['total_tax'])}"})
#     elif parsed["intent"] == "average_compliance":
#         return jsonify({"response": f"{get_average_compliance(lga):.2f}%"})

#     return jsonify({"response": "Could not process your request."})

# # -------------------- /summary API --------------------
# @app.route("/summary", methods=["GET"])
# def summary():
#     taxpayer_income = pd.read_sql("SELECT * FROM taxpayers", engine).groupby('lga')['declared_income'].sum().reset_index()
#     business_income = pd.read_sql("SELECT * FROM businesses", engine).groupby('lga')['annual_revenue'].sum().reset_index()
#     property_income = pd.read_sql("SELECT * FROM properties", engine).groupby('lga')['estimated_value'].sum().reset_index()

#     summary_df = lagos_lga[['lga_name','latitude','longitude']].merge(
#         taxpayer_income, left_on='lga_name', right_on='lga', how='left'
#     ).merge(
#         business_income, left_on='lga_name', right_on='lga', how='left'
#     ).merge(
#         property_income, left_on='lga_name', right_on='lga', how='left'
#     )

#     num_cols = ['declared_income', 'annual_revenue', 'estimated_value']
#     summary_df[num_cols] = summary_df[num_cols].fillna(0)
#     summary_df['total_income'] = summary_df[num_cols].sum(axis=1)

#     highest = summary_df.loc[summary_df['total_income'].idxmax()]
#     lowest = summary_df.loc[summary_df['total_income'].idxmin()]

#     top_business = pd.read_sql(
#         "SELECT occupation, SUM(declared_income) AS total_income FROM taxpayers GROUP BY occupation ORDER BY total_income DESC LIMIT 1;",
#         engine
#     ).iloc[0]

#     response = {
#         "LGA_summary": summary_df[['lga_name','total_income']].to_dict(orient="records"),
#         "highest_LGA": {"name": highest['lga_name'], "total_income": float(highest['total_income'])},
#         "lowest_LGA": {"name": lowest['lga_name'], "total_income": float(lowest['total_income'])},
#         "top_business_type": {"occupation": top_business['occupation'], "total_income": float(top_business['total_income'])},
#         "map": "../data/business_lga_map.html",
#         "recommendations": [
#             f"Focus policy incentives on {top_business['occupation']} sector.",
#             f"Investigate low-performing LGA: {lowest['lga_name']}.",
#             f"Strengthen compliance in high-performing LGA: {highest['lga_name']}."
#         ]
#     }
#     return jsonify(response)

# # -------------------- Run Flask --------------------
# if __name__ == "__main__":
#     port = int(os.getenv("PORT", 10000))
#     app.run(host="0.0.0.0", port=port)



# from flask import Flask, request, jsonify
# import os
# import pandas as pd
# from sqlalchemy import create_engine
# import psycopg2
# from rapidfuzz import process

# # =========================================================
# # Flask app
# # =========================================================
# app = Flask(__name__)

# # =========================================================
# # Database connection
# # =========================================================
# DB_HOST=localhost
# DB_NAME=lagos_revenue
# DB_USER=iwebukeonyeamachi
# DB_PASS=Richiwin

# engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")
# conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
# cur = conn.cursor()

# # =========================================================
# # Load LGAs
# # =========================================================
# current_dir = os.path.dirname(os.path.abspath(__file__))
# csv_path = os.path.join(current_dir, "../data/lagos_lgas.csv")
# lagos_lga = pd.read_csv(csv_path)
# LGA_LIST = [lga.strip().lower() for lga in lagos_lga["lga_name"].tolist()]

# # =========================================================
# # Helpers
# # =========================================================
# def execute_sql(sql):
#     try:
#         cur.execute(sql)
#         return cur.fetchall()
#     except Exception as e:
#         print("SQL ERROR:", e)
#         conn.rollback()
#         return []

# def format_currency(val):
#     if val is None:
#         return "₦0.00"
#     return f"₦{float(val):,.2f}"

# def match_lga(text):
#     text = text.lower()
#     for lga in LGA_LIST:
#         if lga in text:
#             return lga
#     match = process.extractOne(text, LGA_LIST, score_cutoff=75)
#     if match:
#         return match[0]
#     return None

# # =========================================================
# # Question parsing
# # =========================================================
# def parse_question(question):
#     question = question.lower()
#     parsed = {"intent": None, "lga": match_lga(question)}

#     if "total revenue" in question:
#         parsed["intent"] = "total_revenue"
#     elif "total tax" in question:
#         parsed["intent"] = "total_tax"
#     elif "owe" in question or "owing" in question or "tax gap" in question:
#         parsed["intent"] = "tax_gap"
#     elif "most taxed" in question or "top taxpayer" in question:
#         parsed["intent"] = "most_taxed"
#     elif "average compliance" in question:
#         parsed["intent"] = "average_compliance"

#     return parsed

# # =========================================================
# # Data fetching
# # =========================================================
# def get_total_revenue(lga=None):
#     lga_condition = f"WHERE LOWER(lga) = '{lga}'" if lga else ""
#     taxpayer_sum = execute_sql(f"SELECT SUM(declared_income) FROM taxpayers {lga_condition};")[0][0] or 0
#     business_sum = execute_sql(f"SELECT SUM(annual_revenue) FROM businesses {lga_condition};")[0][0] or 0
#     property_sum = execute_sql(f"SELECT SUM(estimated_value) FROM properties {lga_condition};")[0][0] or 0
#     return taxpayer_sum + business_sum + property_sum

# def get_total_tax(lga=None):
#     lga_condition = f"WHERE LOWER(t.lga) = '{lga}'" if lga else ""
#     sql = f"""
#         SELECT SUM(tr.expected_tax)
#         FROM tax_records tr
#         JOIN taxpayers t ON t.id = tr.taxpayer_id
#         {lga_condition};
#     """
#     return execute_sql(sql)[0][0] or 0

# def get_tax_gap(lga=None):
#     lga_condition = f"AND LOWER(t.lga) = '{lga}'" if lga else ""
#     sql = f"""
#         SELECT t.full_name, SUM(tr.expected_tax - tr.tax_paid) AS unpaid_tax
#         FROM taxpayers t
#         JOIN tax_records tr ON t.id = tr.taxpayer_id
#         WHERE tr.tax_paid < tr.expected_tax {lga_condition}
#         GROUP BY t.full_name
#         ORDER BY unpaid_tax DESC
#         LIMIT 5;
#     """
#     rows = execute_sql(sql)
#     if not rows:
#         return []
#     return [{"name": name, "unpaid_tax": float(amount)} for name, amount in rows]

# def get_most_taxed(lga=None):
#     lga_condition = f"WHERE LOWER(t.lga) = '{lga}'" if lga else ""
#     sql = f"""
#         SELECT t.full_name, SUM(tr.expected_tax) AS total_tax
#         FROM taxpayers t
#         JOIN tax_records tr ON t.id = tr.taxpayer_id
#         {lga_condition}
#         GROUP BY t.full_name
#         ORDER BY total_tax DESC
#         LIMIT 1;
#     """
#     rows = execute_sql(sql)
#     if not rows:
#         return {}
#     return {"name": rows[0][0], "total_tax": float(rows[0][1])}

# def get_average_compliance(lga=None):
#     lga_condition = f"WHERE LOWER(lga) = '{lga}'" if lga else ""
#     sql = f"SELECT AVG(compliance_score) FROM taxpayers {lga_condition};"
#     result = execute_sql(sql)[0][0] or 0
#     return float(result)

# # =========================================================
# # Ask API
# # =========================================================
# @app.route("/ask", methods=["POST"])
# def ask():
#     data = request.json
#     question = data.get("question", "")
#     parsed = parse_question(question)
#     lga = parsed.get("lga")

#     if not parsed["intent"]:
#         return jsonify({"response": "Ask for: total revenue, total tax, who owes tax, most taxed, or average compliance."})

#     if parsed["intent"] == "total_revenue":
#         return jsonify({"response": format_currency(get_total_revenue(lga))})

#     elif parsed["intent"] == "total_tax":
#         return jsonify({"response": format_currency(get_total_tax(lga))})

#     elif parsed["intent"] == "tax_gap":
#         gap = get_tax_gap(lga)
#         if not gap:
#             return jsonify({"response": "No records found."})
#         response = "\n".join([f"{r['name']} — {format_currency(r['unpaid_tax'])}" for r in gap])
#         return jsonify({"response": response})

#     elif parsed["intent"] == "most_taxed":
#         most = get_most_taxed(lga)
#         if not most:
#             return jsonify({"response": "No records found."})
#         return jsonify({"response": f"{most['name']} — {format_currency(most['total_tax'])}"})

#     elif parsed["intent"] == "average_compliance":
#         return jsonify({"response": f"{get_average_compliance(lga):.2f}%"})

#     return jsonify({"response": "Could not process your request."})

# # =========================================================
# # Summary API
# # =========================================================
# @app.route("/summary", methods=["GET"])
# def summary():
#     taxpayer_income = pd.read_sql("SELECT * FROM taxpayers", engine).groupby('lga')['declared_income'].sum().reset_index()
#     business_income = pd.read_sql("SELECT * FROM businesses", engine).groupby('lga')['annual_revenue'].sum().reset_index()
#     property_income = pd.read_sql("SELECT * FROM properties", engine).groupby('lga')['estimated_value'].sum().reset_index()

#     summary_df = lagos_lga[['lga_name','latitude','longitude']].merge(
#         taxpayer_income, left_on='lga_name', right_on='lga', how='left'
#     ).merge(
#         business_income, left_on='lga_name', right_on='lga', how='left'
#     ).merge(
#         property_income, left_on='lga_name', right_on='lga', how='left'
#     )

#     num_cols = ['declared_income', 'annual_revenue', 'estimated_value']
#     summary_df[num_cols] = summary_df[num_cols].fillna(0)
#     summary_df['total_income'] = summary_df[num_cols].sum(axis=1)

#     highest = summary_df.loc[summary_df['total_income'].idxmax()]
#     lowest = summary_df.loc[summary_df['total_income'].idxmin()]

#     top_business = pd.read_sql(
#         "SELECT occupation, SUM(declared_income) AS total_income FROM taxpayers GROUP BY occupation ORDER BY total_income DESC LIMIT 1;",
#         engine
#     ).iloc[0]

#     response = {
#         "LGA_summary": summary_df[['lga_name','total_income']].to_dict(orient="records"),
#         "highest_LGA": {"name": highest['lga_name'], "total_income": float(highest['total_income'])},
#         "lowest_LGA": {"name": lowest['lga_name'], "total_income": float(lowest['total_income'])},
#         "top_business_type": {"occupation": top_business['occupation'], "total_income": float(top_business['total_income'])},
#         "map": "../data/business_lga_map.html",
#         "recommendations": [
#             f"Focus policy incentives on {top_business['occupation']} sector.",
#             f"Investigate low-performing LGA: {lowest['lga_name']}.",
#             f"Strengthen compliance in high-performing LGA: {highest['lga_name']}."
#         ]
#     }
#     return jsonify(response)

# # =========================================================
# # Run Flask
# # =========================================================
# if __name__ == "__main__":
#     app.run(debug=True)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    














# import os
# import psycopg2
# from psycopg2.extras import RealDictCursor
# import spacy
# from rapidfuzz import process

# # -------------------- NLP --------------------
# nlp = spacy.load("en_core_web_sm")

# # -------------------- Load LGAs from CSV --------------------
# import pandas as pd
# current_dir = os.path.dirname(os.path.abspath(__file__))
# csv_path = os.path.join(current_dir, "../data/lagos_lgas.csv")

# try:
#     lga_df = pd.read_csv(csv_path)
#     LGA_LIST = [lga.strip().lower() for lga in lga_df["lga_name"].tolist()]
#     print("✅ Loaded LGAs successfully from CSV")
# except Exception as e:
#     print("⚠️ Error loading CSV:", e)
#     LGA_LIST = []

# # -------------------- Helper Functions --------------------
# def format_currency(val):
#     if val is None:
#         return "₦0.00"
#     return f"₦{float(val):,.2f}"

# def match_lga(text):
#     text = text.lower()
#     for lga in LGA_LIST:
#         if lga in text:
#             return lga
#     match = process.extractOne(text, LGA_LIST, score_cutoff=75)
#     if match:
#         return match[0]
#     return None

# def parse_question(question):
#     question = question.lower()
#     parsed = {"intent": None, "lga": match_lga(question)}
#     if "total revenue" in question:
#         parsed["intent"] = "total_revenue"
#     elif "total tax" in question:
#         parsed["intent"] = "total_tax"
#     elif "owe" in question or "owing" in question or "tax gap" in question:
#         parsed["intent"] = "tax_gap"
#     elif "most taxed" in question or "top taxpayer" in question:
#         parsed["intent"] = "most_taxed"
#     elif "average compliance" in question:
#         parsed["intent"] = "average_compliance"
#     return parsed

# def execute_sql(sql, params=None):
#     """Create a fresh connection and cursor per query to avoid cursor/connection issues."""
#     try:
#         conn = psycopg2.connect(
#             host="localhost",
#             database="lagos_revenue",
#             user="iwebukeonyeamachi",
#             password="Richiwin"
#         )
#         cur = conn.cursor(cursor_factory=RealDictCursor)
#         cur.execute(sql, params)
#         rows = cur.fetchall()
#         cur.close()
#         conn.close()
#         return rows
#     except Exception as e:
#         print("SQL ERROR:", e)
#         return []

# # -------------------- Query Functions --------------------
# def get_total_revenue(lga=None):
#     lga_condition = "WHERE LOWER(lga) = %s" if lga else ""
#     params = [lga] if lga else None

#     # Sum each table individually
#     taxpayer_sum = execute_sql(f"SELECT SUM(declared_income) AS total FROM taxpayers {lga_condition};", params)[0]["total"] or 0
#     business_sum = execute_sql(f"SELECT SUM(annual_revenue) AS total FROM businesses {lga_condition};", params)[0]["total"] or 0
#     property_sum = execute_sql(f"SELECT SUM(estimated_value) AS total FROM properties {lga_condition};", params)[0]["total"] or 0

#     return taxpayer_sum + business_sum + property_sum

# def get_most_taxed(lga=None, entity_type=None):
#     """
#     entity_type: 'individual', 'business', 'property', or None for any
#     """
#     params = [lga] if lga else []

#     if entity_type == "business":
#         sql = """
#             SELECT business_name AS name, lga, 'business' AS type,
#                    annual_revenue AS total_tax
#             FROM businesses
#             WHERE (%s IS NULL OR LOWER(lga) = LOWER(%s))
#             ORDER BY annual_revenue DESC
#             LIMIT 1;
#         """
#         params = [lga, lga] if lga else [None, None]

#     elif entity_type == "property":
#         sql = """
#             SELECT CONCAT('Property ID ', id) AS name, lga, 'property' AS type,
#                    estimated_value AS total_tax
#             FROM properties
#             WHERE (%s IS NULL OR LOWER(lga) = LOWER(%s))
#             ORDER BY estimated_value DESC
#             LIMIT 1;
#         """
#         params = [lga, lga] if lga else [None, None]

#     else:  # individual or all
#         sql_ind = """
#             SELECT t.full_name AS name, t.lga, 'individual' AS type,
#                    SUM(tr.expected_tax) AS total_tax
#             FROM taxpayers t
#             JOIN tax_records tr ON t.id = tr.taxpayer_id
#             WHERE (%s IS NULL OR LOWER(t.lga) = LOWER(%s))
#             GROUP BY t.id, t.full_name, t.lga
#             ORDER BY total_tax DESC
#             LIMIT 1;
#         """
#         params_ind = [lga, lga] if lga else [None, None]

#         # fetch top individual, business, property
#         ind = execute_sql(sql_ind, params_ind)
#         bus = execute_sql("""
#             SELECT business_name AS name, lga, 'business' AS type, annual_revenue AS total_tax
#             FROM businesses
#             WHERE (%s IS NULL OR LOWER(lga) = LOWER(%s))
#             ORDER BY annual_revenue DESC
#             LIMIT 1;
#         """, [lga, lga] if lga else [None, None])
#         prop = execute_sql("""
#             SELECT CONCAT('Property ID ', id) AS name, lga, 'property' AS type, estimated_value AS total_tax
#             FROM properties
#             WHERE (%s IS NULL OR LOWER(lga) = LOWER(%s))
#             ORDER BY estimated_value DESC
#             LIMIT 1;
#         """, [lga, lga] if lga else [None, None])

#         # combine and pick top
#         combined = (ind + bus + prop)
#         if not combined:
#             return {"data": [], "intent": "most_taxed", "lga": lga, "message": "No records found."}

#         top = max(combined, key=lambda x: x["total_tax"])
#         return {
#             "data": [{
#                 "name": top["name"],
#                 "lga": top["lga"],
#                 "type": top["type"],
#                 "total_tax": float(top["total_tax"])
#             }],
#             "intent": "most_taxed",
#             "lga": lga
#         }

#     rows = execute_sql(sql, params)
#     if not rows:
#         return {"data": [], "intent": "most_taxed", "lga": lga, "message": "No records found."}

#     r = rows[0]
#     return {
#         "data": [{
#             "name": r["name"],
#             "lga": r["lga"],
#             "type": r["type"],
#             "total_tax": float(r["total_tax"])
#         }],
#         "intent": "most_taxed",
#         "lga": r["lga"]
#     }

# # -------------------- Main Ask Function --------------------
# def ask_question(question):
#     parsed = parse_question(question)
#     lga = parsed.get("lga")

#     if not parsed["intent"]:
#         return "Ask for: total revenue, total tax, who owes tax, most taxed, or average compliance."

#     if parsed["intent"] == "total_revenue":
#         return format_currency(get_total_revenue(lga))
#     elif parsed["intent"] == "most_taxed":
#         return get_most_taxed(lga)
#     else:
#         return "This intent is not yet implemented."

# # -------------------- Flask Example --------------------
# from flask import Flask, request, jsonify
# app = Flask(__name__)

# @app.route("/ask", methods=["POST"])
# def ask():
#     data = request.json
#     question = data.get("question", "")
#     response = ask_question(question)
#     return jsonify(response)

# if __name__ == "__main__":
#     app.run(debug=True)















# # app.py
# from flask import Flask, request, jsonify
# import psycopg2
# import pandas as pd
# import os
# from rapidfuzz import process

# app = Flask(__name__)

# # -------------------- DATABASE CONNECTION --------------------
# DB_HOST = os.getenv("DB_HOST", "localhost")
# DB_NAME = os.getenv("DB_NAME", "lagos_revenue")
# DB_USER = os.getenv("DB_USER", "iwebukeonyeamachi")
# DB_PASS = os.getenv("DB_PASS", "Richiwin")

# def get_conn():
#     return psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)

# # -------------------- UTILITIES --------------------
# def execute_sql(sql, params=None):
#     conn = get_conn()
#     cur = conn.cursor()
#     try:
#         cur.execute(sql, params)
#         rows = cur.fetchall()
#         conn.commit()
#         return rows
#     except Exception as e:
#         print("SQL ERROR:", e)
#         conn.rollback()
#         return []
#     finally:
#         cur.close()
#         conn.close()

# def format_currency(val):
#     if val is None:
#         return "₦0.00"
#     return f"₦{float(val):,.2f}"

# # -------------------- LOAD LGA LIST FROM CSV --------------------
# try:
#     current_dir = os.path.dirname(os.path.abspath(__file__))
#     csv_path = os.path.join(current_dir, "../data/lagos_lgas.csv")
#     lga_df = pd.read_csv(csv_path)
#     LGA_LIST = [lga.strip().lower() for lga in lga_df["lga_name"].tolist()]
#     print("✅ Loaded LGAs from CSV")
# except Exception as e:
#     print("⚠️ Error loading lagos_lgas.csv:", e)
#     LGA_LIST = []

# def match_lga(text):
#     text = text.lower()
#     # Direct match
#     for lga in LGA_LIST:
#         if lga in text:
#             return lga
#     # Fuzzy match
#     match = process.extractOne(text, LGA_LIST, score_cutoff=75)
#     if match:
#         return match[0]
#     return None

# # -------------------- PARSE QUESTION --------------------
# def parse_question(question):
#     q = question.lower()
#     lga = match_lga(q)

#     intent = None
#     if "owe" in q or "owing" in q or "tax gap" in q:
#         intent = "tax_gap"
#     elif "most taxed" in q or "top taxpayer" in q:
#         intent = "most_taxed"
#     elif "total revenue" in q:
#         intent = "total_revenue"
#     elif "total tax" in q:
#         intent = "total_tax"
#     elif "average compliance" in q:
#         intent = "average_compliance"

#     return intent, lga

# # -------------------- API ROUTE --------------------
# @app.route("/ask", methods=["POST"])
# def ask():
#     data = request.json
#     question = data.get("question", "")
#     intent, lga = parse_question(question)

#     if not intent:
#         return jsonify({
#             "data": [],
#             "intent": None,
#             "lga": lga,
#             "message": "Ask for: total revenue, total tax, who owes tax, most taxed, or average compliance."
#         })

#     # Prepare LGA filter
#     lga_filter = ""
#     params = []
#     if lga:
#         lga_filter = "AND LOWER(t.lga) = %s"
#         params.append(lga)

#     # -------------------- TAX GAP --------------------
#     if intent == "tax_gap":
#         sql = f"""
#             SELECT t.full_name, SUM(tr.expected_tax - tr.tax_paid) AS unpaid_tax
#             FROM taxpayers t
#             JOIN tax_records tr ON t.id = tr.taxpayer_id
#             WHERE tr.tax_paid < tr.expected_tax
#             {lga_filter}
#             GROUP BY t.full_name
#             ORDER BY unpaid_tax DESC
#             LIMIT 5;
#         """
#         rows = execute_sql(sql, params if params else None)
#         result = [{"name": r[0], "unpaid_tax": float(r[1])} for r in rows]
#         return jsonify({"data": result, "intent": intent, "lga": lga})

#     # -------------------- MOST TAXED --------------------
#     elif intent == "most_taxed":
#         lga_filter_sql = ""
#         if lga:
#             lga_filter_sql = "WHERE LOWER(t.lga) = %s"
#         sql = f"""
#             SELECT t.full_name, SUM(tr.expected_tax) AS total_tax
#             FROM taxpayers t
#             JOIN tax_records tr ON t.id = tr.taxpayer_id
#             {lga_filter_sql}
#             GROUP BY t.full_name
#             ORDER BY total_tax DESC
#             LIMIT 1;
#         """
#         rows = execute_sql(sql, params if params else None)
#         if not rows:
#             return jsonify({"data": [], "intent": intent, "lga": lga, "message": "No records found."})
#         return jsonify({"data": [{"name": rows[0][0], "total_tax": float(rows[0][1])}], "intent": intent, "lga": lga})

#     # -------------------- TOTAL REVENUE --------------------
#     elif intent == "total_revenue":
#         sql_t = f"SELECT SUM(declared_income) FROM taxpayers WHERE 1=1 {lga_filter};"
#         sql_b = f"SELECT SUM(annual_revenue) FROM businesses WHERE 1=1 {lga_filter};"
#         sql_p = f"SELECT SUM(estimated_value) FROM properties WHERE 1=1 {lga_filter};"

#         taxpayer_sum = execute_sql(sql_t, params if params else None)[0][0] or 0
#         business_sum = execute_sql(sql_b, params if params else None)[0][0] or 0
#         property_sum = execute_sql(sql_p, params if params else None)[0][0] or 0

#         total = taxpayer_sum + business_sum + property_sum
#         return jsonify({"data": {"total_revenue": total}, "intent": intent, "lga": lga})

#     # -------------------- TOTAL TAX --------------------
#     elif intent == "total_tax":
#         sql = f"""
#             SELECT SUM(tr.expected_tax)
#             FROM tax_records tr
#             JOIN taxpayers t ON t.id = tr.taxpayer_id
#             WHERE 1=1 {lga_filter};
#         """
#         total_tax = execute_sql(sql, params if params else None)[0][0] or 0
#         return jsonify({"data": {"total_tax": total_tax}, "intent": intent, "lga": lga})

#     # -------------------- AVERAGE COMPLIANCE --------------------
#     elif intent == "average_compliance":
#         sql = f"SELECT AVG(compliance_score) FROM taxpayers WHERE 1=1 {lga_filter};"
#         avg = execute_sql(sql, params if params else None)[0][0] or 0
#         return jsonify({"data": {"average_compliance": float(avg)}, "intent": intent, "lga": lga})

#     return jsonify({"data": [], "intent": intent, "lga": lga})

# # -------------------- RUN APP --------------------
# if __name__ == "__main__":
#     app.run(debug=True)







# from flask import Flask, request, jsonify
# import psycopg2
# import pandas as pd
# import spacy
# from rapidfuzz import process
# import os

# app = Flask(__name__)

# # -------------------- DATABASE CONNECTION --------------------
# conn = psycopg2.connect(
#     host="localhost",
#     database="lagos_revenue",
#     user="iwebukeonyeamachi",
#     password="Richiwin"
# )

# cur = conn.cursor()

# # -------------------- NLP --------------------
# nlp = spacy.load("en_core_web_sm")

# # -------------------- LOAD LGA LIST --------------------
# try:
#     current_dir = os.path.dirname(os.path.abspath(__file__))
#     csv_path = os.path.join(current_dir, "../data/lagos_lgas.csv")

#     lga_df = pd.read_csv(csv_path)
#     LGA_LIST = [lga.strip().lower() for lga in lga_df["lga_name"].tolist()]

#     print("Loaded LGAs successfully")

# except Exception as e:
#     print("Error loading lagos_lgas.csv:", e)
#     LGA_LIST = []

# # -------------------- UTILITIES --------------------
# def execute_sql(sql):

#     try:
#         cur.execute(sql)
#         rows = cur.fetchall()
#         return rows

#     except Exception as e:

#         print("SQL ERROR:", e)
#         conn.rollback()
#         return []

# def format_currency(val):

#     if val is None:
#         return 0

#     return float(val)

# # -------------------- LGA MATCHING --------------------
# def match_lga(text):

#     text = text.lower()

#     for lga in LGA_LIST:
#         if lga in text:
#             return lga

#     match = process.extractOne(text, LGA_LIST, score_cutoff=75)

#     if match:
#         return match[0]

#     return None


# # ==========================================================
# # 1️⃣ TOTAL REVENUE
# # ==========================================================
# @app.route('/total-revenue', methods=['GET'])
# def total_revenue():

#     lga = request.args.get('lga')

#     if lga:

#         taxpayer_sql = f"SELECT SUM(declared_income) FROM taxpayers WHERE LOWER(lga)=LOWER('{lga}')"
#         business_sql = f"SELECT SUM(annual_revenue) FROM businesses WHERE LOWER(lga)=LOWER('{lga}')"
#         property_sql = f"SELECT SUM(estimated_value) FROM properties WHERE LOWER(lga)=LOWER('{lga}')"

#     else:

#         taxpayer_sql = "SELECT SUM(declared_income) FROM taxpayers"
#         business_sql = "SELECT SUM(annual_revenue) FROM businesses"
#         property_sql = "SELECT SUM(estimated_value) FROM properties"

#     taxpayer_sum = execute_sql(taxpayer_sql)[0][0] or 0
#     business_sum = execute_sql(business_sql)[0][0] or 0
#     property_sum = execute_sql(property_sql)[0][0] or 0

#     total = taxpayer_sum + business_sum + property_sum

#     return jsonify({

#         "total_revenue": format_currency(total),
#         "taxpayer_income": format_currency(taxpayer_sum),
#         "business_income": format_currency(business_sum),
#         "property_income": format_currency(property_sum)

#     })


# # ==========================================================
# # 2️⃣ TAX GAP (WHO OWES TAX)
# # ==========================================================
# @app.route('/tax-gap', methods=['GET'])
# def tax_gap():

#     sql = """
#         SELECT t.full_name,
#                SUM(tr.expected_tax - tr.tax_paid) AS unpaid_tax
#         FROM taxpayers t
#         JOIN tax_records tr
#         ON t.id = tr.taxpayer_id
#         WHERE tr.tax_paid < tr.expected_tax
#         GROUP BY t.full_name
#         ORDER BY unpaid_tax DESC
#         LIMIT 5
#     """

#     rows = execute_sql(sql)

#     results = []

#     for name, amount in rows:

#         results.append({
#             "name": name,
#             "unpaid_tax": float(amount)
#         })

#     return jsonify(results)


# # ==========================================================
# # 3️⃣ MOST TAXED PERSON
# # ==========================================================
# @app.route('/top-taxpayer', methods=['GET'])
# def top_taxpayer():

#     sql = """
#         SELECT t.full_name,
#                SUM(tr.expected_tax) AS total_tax
#         FROM taxpayers t
#         JOIN tax_records tr
#         ON t.id = tr.taxpayer_id
#         GROUP BY t.full_name
#         ORDER BY total_tax DESC
#         LIMIT 1
#     """

#     rows = execute_sql(sql)

#     if rows:

#         name, tax = rows[0]

#         return jsonify({

#             "top_taxpayer": name,
#             "total_tax": float(tax)

#         })

#     return jsonify({"message": "No data found"})


# # ==========================================================
# # 4️⃣ AVERAGE COMPLIANCE
# # ==========================================================
# @app.route('/compliance', methods=['GET'])
# def compliance():

#     sql = "SELECT AVG(compliance_score) FROM taxpayers"

#     rows = execute_sql(sql)

#     avg = rows[0][0] if rows else 0

#     return jsonify({

#         "average_compliance": float(avg)

#     })

# @app.route('/chatbot', methods=['POST'])
# def chatbot():

#     data = request.json
#     question = data.get("question", "").lower()

#     lga = match_lga(question)

#     # ===============================
#     # TOTAL REVENUE
#     # ===============================
#     if "revenue" in question:

#         if lga:
#             sql = f"""
#             SELECT
#             SUM(declared_income)
#             FROM taxpayers
#             WHERE LOWER(lga)='{lga}'
#             """
#         else:
#             sql = "SELECT SUM(declared_income) FROM taxpayers"

#         result = execute_sql(sql)[0][0] or 0

#         return jsonify({
#             "intent": "total_revenue",
#             "lga": lga,
#             "answer": float(result)
#         })


#     # ===============================
#     # TOTAL TAX
#     # ===============================
#     if "total tax" in question:

#         if lga:
#             sql = f"""
#             SELECT SUM(expected_tax)
#             FROM tax_records tr
#             JOIN taxpayers t
#             ON t.id = tr.taxpayer_id
#             WHERE LOWER(t.lga)='{lga}'
#             """
#         else:
#             sql = "SELECT SUM(expected_tax) FROM tax_records"

#         result = execute_sql(sql)[0][0] or 0

#         return jsonify({
#             "intent": "total_tax",
#             "lga": lga,
#             "answer": float(result)
#         })


#     # ===============================
#     # TAX GAP
#     # ===============================
#     if "owe" in question or "tax gap" in question:

#         if lga:

#             sql = f"""
#             SELECT t.full_name,
#             SUM(tr.expected_tax - tr.tax_paid) AS unpaid_tax
#             FROM taxpayers t
#             JOIN tax_records tr
#             ON t.id = tr.taxpayer_id
#             WHERE LOWER(t.lga)='{lga}'
#             AND tr.tax_paid < tr.expected_tax
#             GROUP BY t.full_name
#             ORDER BY unpaid_tax DESC
#             LIMIT 5
#             """

#         else:

#             sql = """
#             SELECT t.full_name,
#             SUM(tr.expected_tax - tr.tax_paid) AS unpaid_tax
#             FROM taxpayers t
#             JOIN tax_records tr
#             ON t.id = tr.taxpayer_id
#             WHERE tr.tax_paid < tr.expected_tax
#             GROUP BY t.full_name
#             ORDER BY unpaid_tax DESC
#             LIMIT 5
#             """

#         rows = execute_sql(sql)

#         results = []

#         for name, amount in rows:
#             results.append({
#                 "name": name,
#                 "unpaid_tax": float(amount)
#             })

#         return jsonify({
#             "intent": "tax_gap",
#             "lga": lga,
#             "results": results
#         })


#     # ===============================
#     # TOP TAXPAYER
#     # ===============================
#     if "most taxed" in question or "top taxpayer" in question:

#         sql = """
#         SELECT t.full_name,
#         SUM(tr.expected_tax) AS total_tax
#         FROM taxpayers t
#         JOIN tax_records tr
#         ON t.id = tr.taxpayer_id
#         GROUP BY t.full_name
#         ORDER BY total_tax DESC
#         LIMIT 1
#         """

#         rows = execute_sql(sql)

#         name, tax = rows[0]

#         return jsonify({
#             "intent": "top_taxpayer",
#             "name": name,
#             "tax": float(tax)
#         })


#     # ===============================
#     # AVERAGE COMPLIANCE
#     # ===============================
#     if "compliance" in question:

#         sql = "SELECT AVG(compliance_score) FROM taxpayers"

#         result = execute_sql(sql)[0][0] or 0

#         return jsonify({
#             "intent": "average_compliance",
#             "answer": float(result)
#         })


#     # ===============================
#     # HIGHEST REVENUE LGA
#     # ===============================
#     if "highest revenue" in question or "top lga" in question:

#         sql = """
#         SELECT lga,
#         SUM(declared_income) AS revenue
#         FROM taxpayers
#         GROUP BY lga
#         ORDER BY revenue DESC
#         LIMIT 1
#         """

#         rows = execute_sql(sql)

#         lga, revenue = rows[0]

#         return jsonify({
#             "intent": "top_lga",
#             "lga": lga,
#             "revenue": float(revenue)
#         })


#     # ===============================
#     # LOWEST REVENUE LGA
#     # ===============================
#     if "lowest revenue" in question:

#         sql = """
#         SELECT lga,
#         SUM(declared_income) AS revenue
#         FROM taxpayers
#         GROUP BY lga
#         ORDER BY revenue ASC
#         LIMIT 1
#         """

#         rows = execute_sql(sql)

#         lga, revenue = rows[0]

#         return jsonify({
#             "intent": "lowest_lga",
#             "lga": lga,
#             "revenue": float(revenue)
#         })


#     return jsonify({
#         "message": "Ask about revenue, tax gap, compliance, or taxpayers"
#     })


# # ==========================================================
# # RUN SERVER
# # ==========================================================
# if __name__ == "__main__":
#     app.run(debug=True)