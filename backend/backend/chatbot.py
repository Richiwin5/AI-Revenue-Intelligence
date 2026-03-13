# chatbot.py
import psycopg2
import os
from intent_engine import parse_question
from functools import lru_cache

# -------------------- DATABASE CONNECTION --------------------
conn = psycopg2.connect(
    host=os.getenv("DB_HOST", "localhost"),
    database=os.getenv("DB_NAME", "lagos_revenue"),
    user=os.getenv("DB_USER", "iwebukeonyeamachi"),
    password=os.getenv("DB_PASS", "Richiwin")
)
cur = conn.cursor()

# -------------------- UTILITIES --------------------
def execute_sql(sql):
    try:
        cur.execute(sql)
        return cur.fetchall()
    except Exception as e:
        print("SQL ERROR:", e)
        conn.rollback()
        return []

def format_currency(val):
    if not val:
        return "₦0.00"
    return f"₦{float(val):,.2f}"

# -------------------- TOTALS --------------------
@lru_cache(maxsize=None)
def get_total_revenue(lga=None):
    """Total revenue from all tables"""
    lga_condition = ""
    if lga:
        lga_condition = f"WHERE LOWER(lga) = '{lga}'"
    # Sum taxpayers
    tax_sql = f"SELECT SUM(declared_income) FROM taxpayers {lga_condition};"
    taxpayer_sum = execute_sql(tax_sql)[0][0] or 0
    # Sum businesses
    bus_sql = f"SELECT SUM(annual_revenue) FROM businesses {lga_condition};"
    business_sum = execute_sql(bus_sql)[0][0] or 0
    # Sum properties
    prop_sql = f"SELECT SUM(estimated_value) FROM properties {lga_condition};"
    property_sum = execute_sql(prop_sql)[0][0] or 0

    return taxpayer_sum + business_sum + property_sum

def get_top_lga():
    sql = """
        SELECT lga, SUM(declared_income) + SUM(annual_revenue) + SUM(estimated_value) AS total
        FROM (
            SELECT lga, declared_income, 0 AS annual_revenue, 0 AS estimated_value FROM taxpayers
            UNION ALL
            SELECT lga, 0, annual_revenue, 0 FROM businesses
            UNION ALL
            SELECT lga, 0, 0, estimated_value FROM properties
        ) t
        GROUP BY lga
        ORDER BY total DESC
        LIMIT 1;
    """
    row = execute_sql(sql)
    if row:
        return {"lga": row[0][0], "total": row[0][1]}
    return None

def get_lowest_lga():
    sql = """
        SELECT lga, SUM(declared_income) + SUM(annual_revenue) + SUM(estimated_value) AS total
        FROM (
            SELECT lga, declared_income, 0 AS annual_revenue, 0 AS estimated_value FROM taxpayers
            UNION ALL
            SELECT lga, 0, annual_revenue, 0 FROM businesses
            UNION ALL
            SELECT lga, 0, 0, estimated_value FROM properties
        ) t
        GROUP BY lga
        ORDER BY total ASC
        LIMIT 1;
    """
    row = execute_sql(sql)
    if row:
        return {"lga": row[0][0], "total": row[0][1]}
    return None

def get_top_sector():
    sql = """
        SELECT occupation, SUM(declared_income) AS total
        FROM taxpayers
        GROUP BY occupation
        ORDER BY total DESC
        LIMIT 1;
    """
    row = execute_sql(sql)
    if row:
        return {"occupation": row[0][0], "total": row[0][1]}
    return None

def get_taxpayer_count(lga=None):
    lga_condition = ""
    if lga:
        lga_condition = f"WHERE LOWER(lga) = '{lga}'"
    sql = f"SELECT COUNT(*) FROM taxpayers {lga_condition};"
    return execute_sql(sql)[0][0] or 0

def get_total_paye(lga=None):
    lga_condition = ""
    if lga:
        lga_condition = f"AND LOWER(t.lga) = '{lga}'"
    sql = f"""
        SELECT SUM(tr.tax_paid) 
        FROM tax_records tr 
        JOIN taxpayers t ON t.id = tr.taxpayer_id
        WHERE tr.tax_paid > 0 {lga_condition};
    """
    return execute_sql(sql)[0][0] or 0

# -------------------- MAIN ASK FUNCTION --------------------
def ask_question(question):
    parsed = parse_question(question)
    intent = parsed.get("intent")
    lga = parsed.get("lga")

    if intent == "total_revenue":
        total = get_total_revenue(lga)
        if lga:
            return f"Total revenue in {lga.title()}: {format_currency(total)}"
        return f"Total revenue in Lagos: {format_currency(total)}"

    elif intent == "top_lga":
        top = get_top_lga()
        if top:
            return f"Top LGA by revenue: {top['lga'].title()} ({format_currency(top['total'])})"
        return "Could not find top LGA."

    elif intent == "lowest_lga":
        low = get_lowest_lga()
        if low:
            return f"Lowest LGA by revenue: {low['lga'].title()} ({format_currency(low['total'])})"
        return "Could not find lowest LGA."

    elif intent == "top_sector":
        top = get_top_sector()
        if top:
            return f"Top business sector: {top['occupation']} ({format_currency(top['total'])})"
        return "Could not find top sector."

    elif intent == "taxpayer_data":
        count = get_taxpayer_count(lga)
        if lga:
            return f"Taxpayer count in {lga.title()}: {count}"
        return f"Total taxpayers in Lagos: {count}"

    elif intent == "business_revenue":
        total = get_total_revenue(lga)
        return f"Total business revenue{f' in {lga.title()}' if lga else ''}: {format_currency(total)}"

    elif intent == "total_tax":
        lga_condition = f"WHERE LOWER(t.lga) = '{lga}'" if lga else ""
        sql = f"SELECT SUM(tr.expected_tax) FROM tax_records tr JOIN taxpayers t ON t.id = tr.taxpayer_id {lga_condition};"
        total = execute_sql(sql)[0][0] or 0
        return f"Total tax collected{f' in {lga.title()}' if lga else ''}: {format_currency(total)}"

    elif intent == "tax_gap":
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
        if not rows:
            return "No records found."
        return "\n".join([f"{name} — {format_currency(amount)}" for name, amount in rows])

    elif intent == "compliance":
        lga_condition = f"WHERE LOWER(lga) = '{lga}'" if lga else ""
        sql = f"SELECT AVG(compliance_score) FROM taxpayers {lga_condition};"
        result = execute_sql(sql)[0][0] or 0
        return f"Average compliance{f' in {lga.title()}' if lga else ''}: {float(result):.2f}%"

    return "I couldn't understand the question. Try asking about revenue, taxpayers, businesses, or properties."

# -------------------- CHAT LOOP --------------------
if __name__ == "__main__":
    print("Lagos Revenue AI Chatbot")
    print("Type exit to quit\n")

    while True:
        user_input = input("You: ")
        if user_input.lower() in ["exit", "quit"]:
            break
        response = ask_question(user_input)
        print("Bot:", response)

# -------------------- CLEANUP --------------------
cur.close()
conn.close()
































# import psycopg2
# import spacy
# from rapidfuzz import process
# import pandas as pd
# import os

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

#     print("Loaded LGAs successfully.")

# except Exception as e:
#     print("Error loading lagos_lgas.csv:", e)
#     LGA_LIST = []

# # -------------------- UTILITIES --------------------
# def execute_sql(sql):
#     try:
#         cur.execute(sql)
#         return cur.fetchall()
#     except Exception as e:
#         print("SQL ERROR:", e)
#         conn.rollback()
#         return []

# def format_currency(val):
#     try:
#         if val is None:
#             return "₦0.00"
#         return f"₦{float(val):,.2f}"
#     except Exception:
#         return "₦0.00"

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
#     question = question.lower()
#     parsed = {
#         "intent": None,
#         "lga": match_lga(question)
#     }
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

# # -------------------- FETCH TOTALS PER LGA --------------------
# def get_total_revenue(lga=None):
#     lga_condition = ""
#     if lga:
#         lga_condition = f"WHERE LOWER(lga) = '{lga}'"

#     # Sum each table individually
#     taxpayer_sql = f"SELECT SUM(declared_income) FROM taxpayers {lga_condition};"
#     business_sql = f"SELECT SUM(annual_revenue) FROM businesses {lga_condition};"
#     property_sql = f"SELECT SUM(estimated_value) FROM properties {lga_condition};"

#     taxpayer_sum = execute_sql(taxpayer_sql)[0][0] or 0
#     business_sum = execute_sql(business_sql)[0][0] or 0
#     property_sum = execute_sql(property_sql)[0][0] or 0

#     total = taxpayer_sum + business_sum + property_sum
#     return total

# # -------------------- MAIN ASK FUNCTION --------------------
# def ask_question(question):
#     parsed = parse_question(question)
#     lga = parsed.get("lga")

#     if not parsed["intent"]:
#         return ("Ask for: total revenue, total tax, who owes tax, "
#                 "most taxed, or average compliance.")

#     if parsed["intent"] == "total_revenue":
#         total = get_total_revenue(lga)
#         return format_currency(total)

#     elif parsed["intent"] == "total_tax":
#         lga_condition = ""
#         if lga:
#             lga_condition = f"WHERE LOWER(t.lga) = '{lga}'"
#         sql = f"SELECT SUM(tr.expected_tax) FROM tax_records tr JOIN taxpayers t ON t.id = tr.taxpayer_id {lga_condition};"
#         result = execute_sql(sql)[0][0] or 0
#         return format_currency(result)

#     elif parsed["intent"] == "tax_gap":
#         lga_condition = ""
#         if lga:
#             lga_condition = f"AND LOWER(t.lga) = '{lga}'"
#         sql = f"""
#             SELECT t.full_name, SUM(tr.expected_tax - tr.tax_paid) AS unpaid_tax
#             FROM taxpayers t
#             JOIN tax_records tr ON t.id = tr.taxpayer_id
#             WHERE tr.tax_paid < tr.expected_tax {lga_condition}
#             GROUP BY t.full_name
#             ORDER BY unpaid_tax DESC
#             LIMIT 5;
#         """
#         rows = execute_sql(sql)
#         if not rows:
#             return "No records found."
#         return "\n".join([f"{name} — {format_currency(amount)}" for name, amount in rows])

#     elif parsed["intent"] == "most_taxed":
#         lga_condition = ""
#         if lga:
#             lga_condition = f"AND LOWER(t.lga) = '{lga}'"
#         sql = f"""
#             SELECT t.full_name, SUM(tr.expected_tax) AS total_tax
#             FROM taxpayers t
#             JOIN tax_records tr ON t.id = tr.taxpayer_id
#             GROUP BY t.full_name
#             {lga_condition}
#             ORDER BY total_tax DESC
#             LIMIT 1;
#         """
#         rows = execute_sql(sql)
#         if not rows:
#             return "No records found."
#         return f"{rows[0][0]} — {format_currency(rows[0][1])}"

#     elif parsed["intent"] == "average_compliance":
#         lga_condition = ""
#         if lga:
#             lga_condition = f"WHERE LOWER(lga) = '{lga}'"
#         sql = f"SELECT AVG(compliance_score) FROM taxpayers {lga_condition};"
#         result = execute_sql(sql)[0][0] or 0
#         return f"{float(result):.2f}%"

#     return "Could not process your request."

# # -------------------- CHAT LOOP --------------------
# if __name__ == "__main__":
#     print("Welcome to Lagos Revenue Intelligence Chatbot (Accurate Version)")
#     print("Type 'exit' to quit.\n")

#     while True:
#         user_input = input("You: ")
#         if user_input.lower() in ["exit", "quit"]:
#             break
#         print("Bot:", ask_question(user_input))

# # -------------------- CLEANUP --------------------
# cur.close()
# conn.close()





















































































































# from flask import Flask, request, jsonify
# import psycopg2
# import pandas as pd
# import spacy
# from rapidfuzz import process
# import os

# app = Flask(__name__)

# # ==============================
# # DATABASE
# # ==============================

# conn = psycopg2.connect(
#     host="localhost",
#     database="lagos_revenue",
#     user="iwebukeonyeamachi",
#     password="Richiwin"
# )

# cur = conn.cursor()

# # ==============================
# # NLP
# # ==============================

# nlp = spacy.load("en_core_web_sm")

# # ==============================
# # LOAD LGAs
# # ==============================

# current_dir = os.path.dirname(os.path.abspath(__file__))
# csv_path = os.path.join(current_dir, "../data/lagos_lgas.csv")

# lga_df = pd.read_csv(csv_path)
# LGA_LIST = [lga.lower() for lga in lga_df["lga_name"]]

# # ==============================
# # UTILITIES
# # ==============================

# def execute_sql(sql):

#     try:
#         cur.execute(sql)
#         return cur.fetchall()

#     except Exception as e:
#         print("SQL ERROR:", e)
#         conn.rollback()
#         return []


# def match_lga(text):

#     text = text.lower()

#     for lga in LGA_LIST:
#         if lga in text:
#             return lga

#     match = process.extractOne(text, LGA_LIST, score_cutoff=75)

#     if match:
#         return match[0]

#     return None


# # ==============================
# # INTENT DETECTOR
# # ==============================

# def detect_intent(question):

#     question = question.lower()

#     if "highest revenue" in question or "most revenue" in question or "top lga" in question:
#         return "top_lga"

#     if "lowest revenue" in question or "least revenue" in question:
#         return "lowest_lga"

#     if "total revenue" in question or "how much revenue" in question:
#         return "total_revenue"

#     if "business sector" in question or "occupation" in question or "industry" in question:
#         return "top_sector"

#     if "owe tax" in question or "tax gap" in question:
#         return "tax_gap"

#     if "most taxed person" in question or "top taxpayer" in question:
#         return "top_taxpayer"

#     if "compliance" in question:
#         return "compliance"

#     return "unknown"


# # ==============================
# # QUERY ENGINE
# # ==============================

# def run_query(intent, lga):

#     # -------------------
#     # TOTAL REVENUE
#     # -------------------

#     if intent == "total_revenue":

#         if lga:

#             sql = f"""
#             SELECT
#             SUM(t.declared_income) +
#             SUM(b.annual_revenue) +
#             SUM(p.estimated_value)
#             FROM taxpayers t
#             LEFT JOIN businesses b ON b.lga=t.lga
#             LEFT JOIN properties p ON p.lga=t.lga
#             WHERE LOWER(t.lga)='{lga}'
#             """

#         else:

#             sql = """
#             SELECT
#             SUM(declared_income) FROM taxpayers
#             """

#         result = execute_sql(sql)[0][0]

#         return {"total_revenue": float(result or 0)}

#     # -------------------
#     # TOP LGA
#     # -------------------

#     if intent == "top_lga":

#         sql = """
#         SELECT lga,
#         SUM(declared_income) AS revenue
#         FROM taxpayers
#         GROUP BY lga
#         ORDER BY revenue DESC
#         LIMIT 1
#         """

#         row = execute_sql(sql)[0]

#         return {
#             "lga": row[0],
#             "revenue": float(row[1])
#         }

#     # -------------------
#     # LOWEST LGA
#     # -------------------

#     if intent == "lowest_lga":

#         sql = """
#         SELECT lga,
#         SUM(declared_income) AS revenue
#         FROM taxpayers
#         GROUP BY lga
#         ORDER BY revenue ASC
#         LIMIT 1
#         """

#         row = execute_sql(sql)[0]

#         return {
#             "lga": row[0],
#             "revenue": float(row[1])
#         }

#     # -------------------
#     # TAX GAP
#     # -------------------

#     if intent == "tax_gap":

#         lga_condition = ""

#         if lga:
#             lga_condition = f"AND LOWER(t.lga)='{lga}'"

#         sql = f"""
#         SELECT t.full_name,
#         SUM(tr.expected_tax - tr.tax_paid)
#         FROM taxpayers t
#         JOIN tax_records tr
#         ON t.id = tr.taxpayer_id
#         WHERE tr.tax_paid < tr.expected_tax
#         {lga_condition}
#         GROUP BY t.full_name
#         ORDER BY SUM(tr.expected_tax - tr.tax_paid) DESC
#         LIMIT 5
#         """

#         rows = execute_sql(sql)

#         results = []

#         for name, amount in rows:

#             results.append({
#                 "name": name,
#                 "unpaid_tax": float(amount)
#             })

#         return results

#     # -------------------
#     # TOP TAXPAYER
#     # -------------------

#     if intent == "top_taxpayer":

#         sql = """
#         SELECT t.full_name,
#         SUM(tr.expected_tax)
#         FROM taxpayers t
#         JOIN tax_records tr
#         ON t.id = tr.taxpayer_id
#         GROUP BY t.full_name
#         ORDER BY SUM(tr.expected_tax) DESC
#         LIMIT 1
#         """

#         row = execute_sql(sql)[0]

#         return {
#             "name": row[0],
#             "tax": float(row[1])
#         }

#     # -------------------
#     # TOP SECTOR
#     # -------------------

#     if intent == "top_sector":

#         sql = """
#         SELECT occupation,
#         SUM(declared_income)
#         FROM taxpayers
#         GROUP BY occupation
#         ORDER BY SUM(declared_income) DESC
#         LIMIT 5
#         """

#         rows = execute_sql(sql)

#         results = []

#         for occ, income in rows:

#             results.append({
#                 "sector": occ,
#                 "revenue": float(income)
#             })

#         return results

#     # -------------------
#     # COMPLIANCE
#     # -------------------

#     if intent == "compliance":

#         sql = "SELECT AVG(compliance_score) FROM taxpayers"

#         val = execute_sql(sql)[0][0]

#         return {
#             "average_compliance": float(val)
#         }

#     return {"message": "Intent not supported"}


# # ==============================
# # CHATBOT API
# # ==============================

# @app.route("/chatbot", methods=["POST"])
# def chatbot():

#     question = request.json["question"]

#     lga = match_lga(question)

#     intent = detect_intent(question)

#     result = run_query(intent, lga)

#     return jsonify({

#         "question": question,
#         "intent": intent,
#         "lga": lga,
#         "data": result

#     })


# # ==============================
# # RUN SERVER
# # ==============================

# if __name__ == "__main__":
#     app.run(debug=True)









# import psycopg2
# import spacy
# from rapidfuzz import process
# import pandas as pd
# import os

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

#     print("Loaded LGAs successfully.")

# except Exception as e:
#     print("Error loading lagos_lgas.csv:", e)
#     LGA_LIST = []

# # -------------------- UTILITIES --------------------
# def execute_sql(sql):
#     try:
#         cur.execute(sql)
#         return cur.fetchall()
#     except Exception as e:
#         print("SQL ERROR:", e)
#         conn.rollback()
#         return []

# def format_currency(val):
#     try:
#         if val is None:
#             return "₦0.00"
#         return f"₦{float(val):,.2f}"
#     except Exception:
#         return "₦0.00"

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
#     question = question.lower()

#     parsed = {
#         "intent": None,
#         "lga": match_lga(question)
#     }

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

# # -------------------- SQL GENERATOR --------------------
# def generate_sql(parsed):
#     intent = parsed["intent"]
#     lga = parsed["lga"]

#     lga_condition = ""
#     if lga:
#         lga_condition = f"WHERE LOWER(t.lga) = '{lga}'"

#     # TOTAL REVENUE
#     if intent == "total_revenue":
#         return f"""
#             SELECT SUM(
#                 COALESCE(t.declared_income,0) +
#                 COALESCE(t.property_value,0) +
#                 COALESCE(b.annual_revenue,0)
#             )
#             FROM taxpayers t
#             LEFT JOIN businesses b ON b.id = t.id
#             {lga_condition};
#         """

#     # TOTAL TAX
#     if intent == "total_tax":
#         return f"""
#             SELECT SUM(tr.expected_tax)
#             FROM tax_records tr
#             JOIN taxpayers t ON t.id = tr.taxpayer_id
#             {lga_condition};
#         """

#     # TAX GAP (WHO OWES)
#     if intent == "tax_gap":
#         return f"""
#             SELECT t.full_name,
#                    SUM(tr.expected_tax - tr.tax_paid) AS unpaid_tax
#             FROM taxpayers t
#             JOIN tax_records tr ON t.id = tr.taxpayer_id
#             {lga_condition}
#               AND tr.tax_paid < tr.expected_tax
#             GROUP BY t.full_name
#             ORDER BY unpaid_tax DESC
#             LIMIT 5;
#         """

#     # MOST TAXED
#     if intent == "most_taxed":
#         return f"""
#             SELECT t.full_name,
#                    SUM(tr.expected_tax) AS total_tax
#             FROM taxpayers t
#             JOIN tax_records tr ON t.id = tr.taxpayer_id
#             {lga_condition}
#             GROUP BY t.full_name
#             ORDER BY total_tax DESC
#             LIMIT 1;
#         """

#     # AVERAGE COMPLIANCE
#     if intent == "average_compliance":
#         return f"""
#             SELECT AVG(t.compliance_score)
#             FROM taxpayers t
#             {lga_condition};
#         """

#     return None


# # -------------------- MAIN ASK FUNCTION --------------------
# def ask_question(question):
#     parsed = parse_question(question)

#     if not parsed["intent"]:
#         return ("Ask for: total revenue, total tax, who owes tax, "
#                 "most taxed, or average compliance.")

#     sql = generate_sql(parsed)

#     if not sql:
#         return "Could not generate query."

#     rows = execute_sql(sql)

#     if not rows:
#         return "No data found."

#     # Aggregated
#     if parsed["intent"] in ["total_revenue", "total_tax"]:
#         return format_currency(rows[0][0])

#     if parsed["intent"] == "average_compliance":
#         return f"{float(rows[0][0] or 0):.2f}%"

#     # List results
#     if parsed["intent"] in ["tax_gap", "most_taxed"]:
#         response = ""
#         for name, amount in rows:
#             response += f"{name} — {format_currency(amount)}\n"
#         return response if response else "No records found."

# # -------------------- CHAT LOOP --------------------
# if __name__ == "__main__":
#     print("Welcome to Lagos Revenue Intelligence Chatbot (Stable Final Version)")
#     print("Type 'exit' to quit.\n")

#     while True:
#         user_input = input("You: ")
#         if user_input.lower() in ["exit", "quit"]:
#             break
#         print("Bot:", ask_question(user_input))

# # -------------------- CLEANUP --------------------
# cur.close()
# conn.close()


# import psycopg2
# import spacy
# from rapidfuzz import process
# import pandas as pd
# import os

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
#     csv_path = os.path.join(current_dir, "l../data/agos_lgas.csv")

#     lga_df = pd.read_csv(csv_path)
#     LGA_LIST = [lga.strip().lower() for lga in lga_df["lga_name"].tolist()]

#     print("Loaded LGAs successfully.")

# except Exception as e:
#     print("Error loading lagos_lgas.csv:", e)
#     LGA_LIST = []

# # -------------------- UTILITIES --------------------
# def execute_sql(sql):
#     try:
#         cur.execute(sql)
#         return cur.fetchall()
#     except Exception as e:
#         print("SQL ERROR:", e)
#         conn.rollback()
#         return []

# def format_currency(val):
#     try:
#         if val is None:
#             return "₦0.00"
#         return f"₦{float(val):,.2f}"
#     except Exception:
#         return "₦0.00"

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
#     question = question.lower()

#     parsed = {
#         "intent": None,
#         "lga": match_lga(question)
#     }

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

# # -------------------- SQL GENERATOR --------------------
# def generate_sql(parsed):
#     intent = parsed["intent"]
#     lga = parsed["lga"]

#     lga_condition = ""
#     if lga:
#         lga_condition = f"WHERE LOWER(t.lga) = '{lga}'"

#     # TOTAL REVENUE
#     if intent == "total_revenue":
#         return f"""
#             SELECT SUM(
#                 COALESCE(t.declared_income,0) +
#                 COALESCE(t.property_value,0) +
#                 COALESCE(b.annual_revenue,0)
#             )
#             FROM taxpayers t
#             LEFT JOIN businesses b ON b.id = t.id
#             {lga_condition};
#         """

#     # TOTAL TAX
#     if intent == "total_tax":
#         return f"""
#             SELECT SUM(tr.expected_tax)
#             FROM tax_records tr
#             JOIN taxpayers t ON t.id = tr.taxpayer_id
#             {lga_condition};
#         """

#     # TAX GAP (WHO OWES)
#     if intent == "tax_gap":
#         return f"""
#             SELECT t.full_name,
#                    SUM(tr.expected_tax - tr.tax_paid) AS unpaid_tax
#             FROM taxpayers t
#             JOIN tax_records tr ON t.id = tr.taxpayer_id
#             {lga_condition}
#               AND tr.tax_paid < tr.expected_tax
#             GROUP BY t.full_name
#             ORDER BY unpaid_tax DESC
#             LIMIT 5;
#         """

#     # MOST TAXED
#     if intent == "most_taxed":
#         return f"""
#             SELECT t.full_name,
#                    SUM(tr.expected_tax) AS total_tax
#             FROM taxpayers t
#             JOIN tax_records tr ON t.id = tr.taxpayer_id
#             {lga_condition}
#             GROUP BY t.full_name
#             ORDER BY total_tax DESC
#             LIMIT 5;
#         """

#     # AVERAGE COMPLIANCE
#     if intent == "average_compliance":
#         return f"""
#             SELECT AVG(t.compliance_score)
#             FROM taxpayers t
#             {lga_condition};
#         """

#     return None

# # -------------------- MAIN ASK FUNCTION --------------------
# def ask_question(question):
#     parsed = parse_question(question)

#     if not parsed["intent"]:
#         return ("Ask for: total revenue, total tax, who owes tax, "
#                 "most taxed, or average compliance.")

#     sql = generate_sql(parsed)

#     if not sql:
#         return "Could not generate query."

#     rows = execute_sql(sql)

#     if not rows:
#         return "No data found."

#     # Aggregated
#     if parsed["intent"] in ["total_revenue", "total_tax"]:
#         return format_currency(rows[0][0])

#     if parsed["intent"] == "average_compliance":
#         return f"{float(rows[0][0] or 0):.2f}%"

#     # List results
#     if parsed["intent"] in ["tax_gap", "most_taxed"]:
#         response = ""
#         for name, amount in rows:
#             response += f"{name} — {format_currency(amount)}\n"
#         return response if response else "No records found."

# # -------------------- CHAT LOOP --------------------
# if __name__ == "__main__":
#     print("Welcome to Lagos Revenue Intelligence Chatbot (Stable Final Version)")
#     print("Type 'exit' to quit.\n")

#     while True:
#         user_input = input("You: ")
#         if user_input.lower() in ["exit", "quit"]:
#             break
#         print("Bot:", ask_question(user_input))

# # -------------------- CLEANUP --------------------
# cur.close()
# conn.close()

