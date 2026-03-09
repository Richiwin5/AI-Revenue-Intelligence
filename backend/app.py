from flask import Flask, request, jsonify
from flask_cors import CORS
import os, pandas as pd
from sqlalchemy import create_engine
import psycopg2
from rapidfuzz import process
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app)  # <-- enable CORS for all endpoints

# DB setup
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASS
)
cur = conn.cursor()
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# -------------------- Load LGAs --------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(current_dir, "../data/lagos_lgas.csv")
lagos_lga = pd.read_csv(csv_path)
LGA_LIST = [lga.strip().lower() for lga in lagos_lga["lga_name"].tolist()]

# -------------------- Helpers --------------------
def execute_sql(sql):
    try:
        cur.execute(sql)
        return cur.fetchall()
    except Exception as e:
        print("SQL ERROR:", e)
        conn.rollback()
        return []

def format_currency(val):
    if val is None:
        return "₦0.00"
    return f"₦{float(val):,.2f}"

def match_lga(text):
    text = text.lower()
    for lga in LGA_LIST:
        if lga in text:
            return lga
    match = process.extractOne(text, LGA_LIST, score_cutoff=75)
    if match:
        return match[0]
    return None

# -------------------- Question parsing --------------------
def parse_question(question):
    question = question.lower()
    parsed = {"intent": None, "lga": match_lga(question)}

    if "total revenue" in question:
        parsed["intent"] = "total_revenue"
    elif "total tax" in question:
        parsed["intent"] = "total_tax"
    elif "owe" in question or "owing" in question or "tax gap" in question:
        parsed["intent"] = "tax_gap"
    elif "most taxed" in question or "top taxpayer" in question:
        parsed["intent"] = "most_taxed"
    elif "average compliance" in question:
        parsed["intent"] = "average_compliance"

    return parsed

# -------------------- Data fetching --------------------
def get_total_revenue(lga=None):
    lga_condition = f"WHERE LOWER(lga) = '{lga}'" if lga else ""
    taxpayer_sum = execute_sql(f"SELECT SUM(declared_income) FROM taxpayers {lga_condition};")[0][0] or 0
    business_sum = execute_sql(f"SELECT SUM(annual_revenue) FROM businesses {lga_condition};")[0][0] or 0
    property_sum = execute_sql(f"SELECT SUM(estimated_value) FROM properties {lga_condition};")[0][0] or 0
    return taxpayer_sum + business_sum + property_sum

def get_total_tax(lga=None):
    lga_condition = f"WHERE LOWER(t.lga) = '{lga}'" if lga else ""
    sql = f"""
        SELECT SUM(tr.expected_tax)
        FROM tax_records tr
        JOIN taxpayers t ON t.id = tr.taxpayer_id
        {lga_condition};
    """
    return execute_sql(sql)[0][0] or 0

def get_tax_gap(lga=None):
    lga_condition = f"AND LOWER(t.lga) = '{lga}'" if lga else ""
    sql = f"""
        SELECT t.full_name, SUM(tr.expected_tax - tr.tax_paid) AS unpaid_tax
        FROM taxpayers t
        JOIN tax_records tr ON t.id = tr.taxpayer_id
        WHERE tr.tax_paid < tr.expected_tax {lga_condition}
        GROUP BY t.full_name
        ORDER BY unpaid_tax DESC
        LIMIT 5;
    """
    rows = execute_sql(sql)
    return [{"name": name, "unpaid_tax": float(amount)} for name, amount in rows] if rows else []

def get_most_taxed(lga=None):
    lga_condition = f"WHERE LOWER(t.lga) = '{lga}'" if lga else ""
    sql = f"""
        SELECT t.full_name, SUM(tr.expected_tax) AS total_tax
        FROM taxpayers t
        JOIN tax_records tr ON t.id = tr.taxpayer_id
        {lga_condition}
        GROUP BY t.full_name
        ORDER BY total_tax DESC
        LIMIT 1;
    """
    rows = execute_sql(sql)
    return {"name": rows[0][0], "total_tax": float(rows[0][1])} if rows else {}

def get_average_compliance(lga=None):
    lga_condition = f"WHERE LOWER(lga) = '{lga}'" if lga else ""
    sql = f"SELECT AVG(compliance_score) FROM taxpayers {lga_condition};"
    result = execute_sql(sql)[0][0] or 0
    return float(result)

# -------------------- /ask API --------------------
@app.route("/ask", methods=["POST"])
def ask():
    data = request.json
    question = data.get("question", "")
    parsed = parse_question(question)
    lga = parsed.get("lga")

    if not parsed["intent"]:
        return jsonify({"response": "Ask for: total revenue, total tax, who owes tax, most taxed, or average compliance."})

    if parsed["intent"] == "total_revenue":
        return jsonify({"response": format_currency(get_total_revenue(lga))})
    elif parsed["intent"] == "total_tax":
        return jsonify({"response": format_currency(get_total_tax(lga))})
    elif parsed["intent"] == "tax_gap":
        gap = get_tax_gap(lga)
        if not gap:
            return jsonify({"response": "No records found."})
        response = "\n".join([f"{r['name']} — {format_currency(r['unpaid_tax'])}" for r in gap])
        return jsonify({"response": response})
    elif parsed["intent"] == "most_taxed":
        most = get_most_taxed(lga)
        if not most:
            return jsonify({"response": "No records found."})
        return jsonify({"response": f"{most['name']} — {format_currency(most['total_tax'])}"})
    elif parsed["intent"] == "average_compliance":
        return jsonify({"response": f"{get_average_compliance(lga):.2f}%"})

    return jsonify({"response": "Could not process your request."})

# -------------------- /summary API --------------------
@app.route("/summary", methods=["GET"])
def summary():
    taxpayer_income = pd.read_sql("SELECT * FROM taxpayers", engine).groupby('lga')['declared_income'].sum().reset_index()
    business_income = pd.read_sql("SELECT * FROM businesses", engine).groupby('lga')['annual_revenue'].sum().reset_index()
    property_income = pd.read_sql("SELECT * FROM properties", engine).groupby('lga')['estimated_value'].sum().reset_index()

    summary_df = lagos_lga[['lga_name','latitude','longitude']].merge(
        taxpayer_income, left_on='lga_name', right_on='lga', how='left'
    ).merge(
        business_income, left_on='lga_name', right_on='lga', how='left'
    ).merge(
        property_income, left_on='lga_name', right_on='lga', how='left'
    )

    num_cols = ['declared_income', 'annual_revenue', 'estimated_value']
    summary_df[num_cols] = summary_df[num_cols].fillna(0)
    summary_df['total_income'] = summary_df[num_cols].sum(axis=1)

    highest = summary_df.loc[summary_df['total_income'].idxmax()]
    lowest = summary_df.loc[summary_df['total_income'].idxmin()]

    top_business = pd.read_sql(
        "SELECT occupation, SUM(declared_income) AS total_income FROM taxpayers GROUP BY occupation ORDER BY total_income DESC LIMIT 1;",
        engine
    ).iloc[0]

    response = {
        "LGA_summary": summary_df[['lga_name','total_income']].to_dict(orient="records"),
        "highest_LGA": {"name": highest['lga_name'], "total_income": float(highest['total_income'])},
        "lowest_LGA": {"name": lowest['lga_name'], "total_income": float(lowest['total_income'])},
        "top_business_type": {"occupation": top_business['occupation'], "total_income": float(top_business['total_income'])},
        "map": "../data/business_lga_map.html",
        "recommendations": [
            f"Focus policy incentives on {top_business['occupation']} sector.",
            f"Investigate low-performing LGA: {lowest['lga_name']}.",
            f"Strengthen compliance in high-performing LGA: {highest['lga_name']}."
        ]
    }
    return jsonify(response)

# -------------------- Run Flask --------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)



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