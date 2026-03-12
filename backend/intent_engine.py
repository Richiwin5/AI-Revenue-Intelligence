# intent_engine.py
import os
import pandas as pd
from rapidfuzz import process

# -------------------- LOAD LGAs --------------------
def load_lga_list():
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(current_dir, "../data/lagos_lgas.csv")
        df = pd.read_csv(csv_path)
        return [lga.strip().lower() for lga in df["lga_name"].tolist()]
    except Exception as e:
        print("Error loading LGA list:", e)
        return []

LGA_LIST = load_lga_list()

# -------------------- EXTRACT LGA --------------------
def extract_lga(text):
    """
    Extract the LGA from a given text using direct or fuzzy matching
    """
    if not text:
        return None
    text = text.lower()
    for lga in LGA_LIST:
        if lga in text:
            return lga
    # Fuzzy match
    match = process.extractOne(text, LGA_LIST, score_cutoff=70)
    return match[0] if match else None

# -------------------- DETECT INTENT --------------------
def detect_intent(text):
    """
    Detects the intent of the user based on keywords
    """
    if not text:
        return "unknown"

    text = text.lower()

    # ---------------- Revenue / Tax / Compliance ----------------
    if any(k in text for k in [
        "total revenue", "revenue collected", "generate", "revenue from",
        "total income", "income generated", "total generated", "money collected",
        "funds collected", "gross revenue", "income of lga", "revenue report"
    ]):
        return "total_revenue"

    if any(k in text for k in [
        "total tax","total debt", "tax collected", "taxes", "tax paid",
        "tax amount", "total payable", "paye collected", "tax income"
    ]):
        return "total_tax"

    if any(k in text for k in [
        "who owes", "tax gap", "owing tax", "unpaid tax", "tax debt",
        "tax debtor", "outstanding taxpayer", "debtors", "arrears", "unsettled tax"
    ]):
        return "tax_gap"

    if any(k in text for k in [
        "top taxpayer", "highest taxpayer", "biggest taxpayer", "highest debtor",
        "largest debtor", "most owing", "most in debt"
    ]):
        return "top_taxpayer"

    if any(k in text for k in [
        "compliance", "average compliance", "tax compliance", "compliance score",
        "compliant", "non-compliant", "compliance report"
    ]):
        return "compliance"

    # ---------------- Top / Bottom LGA ----------------
    if any(k in text for k in [
        "highest revenue", "top lga", "max revenue", "most revenue",
        "generate the most", "total income", "highest income",
        "biggest earning", "top performing lga", "richest lga", "best lga"
    ]):
        return "top_lga"

    if any(k in text for k in [
        "lowest revenue", "least revenue", "min revenue", "smallest revenue",
        "lowest income", "least income", "poorest lga", "underperforming lga"
    ]):
        return "lowest_lga"

    # ---------------- Business / Occupation ----------------
    if any(k in text for k in [
        "business sector", "occupation", "top sector", "top business",
        "best sector", "most profitable sector", "industry revenue", "sector income"
    ]):
        return "top_sector"

    if any(k in text for k in [
        "business revenue", "sector revenue", "income", "business income",
        "sector income", "earnings", "profit", "business earnings"
    ]):
        return "business_revenue"

    # ---------------- Tables ----------------
    if any(k in text for k in [
        "taxpayer", "taxpayers", "debtors", "debt", "taxpayer list",
        "taxpayer report", "taxpayer info", "taxpayer count"
    ]):
        return "taxpayer_data"

    if any(k in text for k in [
        "business", "businesses", "companies", "company list", "business info"
    ]):
        return "business_data"

    if any(k in text for k in [
        "property", "properties", "estate", "land", "building", "property value"
    ]):
        return "property_data"

    return "unknown"

# -------------------- PARSE QUESTION --------------------
def parse_question(text):
    """
    Parses a question and returns:
    - intent
    - lga (if mentioned)
    """
    lga = extract_lga(text)
    intent = detect_intent(text)
    return {"intent": intent, "lga": lga}








# # intent_engine.py
# import os
# import pandas as pd
# from rapidfuzz import process

# # -------------------- LOAD LGAs --------------------
# def load_lga_list():
#     try:
#         current_dir = os.path.dirname(os.path.abspath(__file__))
#         csv_path = os.path.join(current_dir, "../data/lagos_lgas.csv")
#         df = pd.read_csv(csv_path)
#         return [lga.strip().lower() for lga in df["lga_name"].tolist()]
#     except Exception as e:
#         print("Error loading LGA list:", e)
#         return []

# LGA_LIST = load_lga_list()

# # -------------------- EXTRACT LGA --------------------
# def extract_lga(text):
#     """
#     Extract the LGA from a given text using direct or fuzzy matching
#     """
#     if not text:
#         return None
#     text = text.lower()
#     for lga in LGA_LIST:
#         if lga in text:
#             return lga
#     # Fuzzy match
#     match = process.extractOne(text, LGA_LIST, score_cutoff=75)
#     return match[0] if match else None

# # -------------------- DETECT INTENT --------------------
# def detect_intent(text):
#     """
#     Detects the intent of the user based on keywords
#     """
#     if not text:
#         return "unknown"

#     text = text.lower()

#     # ---------------- Revenue / Tax / Compliance ----------------
#     if any(k in text for k in ["total revenue", "revenue collected", "generate", "revenue from", "total income"]):
#         return "total_revenue"
#     if any(k in text for k in ["total tax","total debt", "tax collected", "taxes"]):
#         return "total_tax"
#     if any(k in text for k in ["who owes", "tax gap", "owing tax", "unpaid tax", "tax debt", "tax debtor", "outstanding taxpayer", "debtors",]):
#         return "tax_gap"
#     if any(k in text for k in ["top taxpayer", "highest taxpayer", "biggest taxpayer", "highest debtor"]):
#         return "top_taxpayer"
#     if any(k in text for k in ["compliance", "average compliance"]):
#         return "compliance"

#     # ---------------- Top / Bottom LGA ----------------
#     if any(k in text for k in ["highest revenue", "top lga", "max revenue", "most revenue", "generate the most", "total income", "highest income", ]):
#         return "top_lga"
#     if any(k in text for k in ["lowest revenue", "least revenue", "min revenue", "smallest revenue", "lowest income", "least income"]):
#         return "lowest_lga"

#     # ---------------- Business / Occupation ----------------
#     if any(k in text for k in ["business sector", "occupation", "top sector", "top business"]):
#         return "top_sector"
#     if any(k in text for k in ["business revenue", "sector revenue", "income", "business income", "sector income"]):
#         return "business_revenue"

#     # ---------------- Tables ----------------
#     if any(k in text for k in ["taxpayer", "taxpayers", "debtors", "debt"]):
#         return "taxpayer_data"
#     if any(k in text for k in ["business", "businesses"]):
#         return "business_data"
#     if any(k in text for k in ["property", "properties"]):
#         return "property_data"

#     return "unknown"

# # -------------------- PARSE QUESTION --------------------
# def parse_question(text):
#     """
#     Parses a question and returns:
#     - intent
#     - lga (if mentioned)
#     """
#     lga = extract_lga(text)
#     intent = detect_intent(text)
#     return {"intent": intent, "lga": lga}





































# # intent_engine.py
# import os
# import pandas as pd
# from rapidfuzz import process

# # -------------------- LOAD LGAs --------------------
# def load_lga_list():
#     try:
#         current_dir = os.path.dirname(os.path.abspath(__file__))
#         csv_path = os.path.join(current_dir, "../data/lagos_lgas.csv")
#         df = pd.read_csv(csv_path)
#         return [lga.strip().lower() for lga in df["lga_name"].tolist()]
#     except:
#         return []

# LGA_LIST = load_lga_list()

# # -------------------- EXTRACT LGA --------------------
# def extract_lga(text):
#     if not text:
#         return None
#     text = text.lower()
#     for lga in LGA_LIST:
#         if lga in text:
#             return lga
#     match = process.extractOne(text, LGA_LIST, score_cutoff=75)
#     return match[0] if match else None

# # -------------------- DETECT INTENT --------------------
# def detect_intent(text):
#     if not text:
#         return "unknown"
#     text = text.lower()
#     if any(k in text for k in ["total revenue", "revenue collected"]):
#         return "total_revenue"
#     if any(k in text for k in ["total tax", "tax collected"]):
#         return "total_tax"
#     if any(k in text for k in ["who owes", "tax gap", "owing tax"]):
#         return "tax_gap"
#     if any(k in text for k in ["top taxpayer", "highest taxpayer"]):
#         return "top_taxpayer"
#     if any(k in text for k in ["compliance", "average compliance"]):
#         return "compliance"
#     if any(k in text for k in ["highest revenue", "top lga"]):
#         return "top_lga"
#     if any(k in text for k in ["lowest revenue", "least revenue"]):
#         return "lowest_lga"
#     if any(k in text for k in ["business sector", "occupation", "top sector"]):
#         return "top_sector"
#     return "unknown"