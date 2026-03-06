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
    except:
        return []

LGA_LIST = load_lga_list()

# -------------------- EXTRACT LGA --------------------
def extract_lga(text):
    if not text:
        return None
    text = text.lower()
    for lga in LGA_LIST:
        if lga in text:
            return lga
    match = process.extractOne(text, LGA_LIST, score_cutoff=75)
    return match[0] if match else None

# -------------------- DETECT INTENT --------------------
def detect_intent(text):
    if not text:
        return "unknown"
    text = text.lower()
    if any(k in text for k in ["total revenue", "revenue collected"]):
        return "total_revenue"
    if any(k in text for k in ["total tax", "tax collected"]):
        return "total_tax"
    if any(k in text for k in ["who owes", "tax gap", "owing tax"]):
        return "tax_gap"
    if any(k in text for k in ["top taxpayer", "highest taxpayer"]):
        return "top_taxpayer"
    if any(k in text for k in ["compliance", "average compliance"]):
        return "compliance"
    if any(k in text for k in ["highest revenue", "top lga"]):
        return "top_lga"
    if any(k in text for k in ["lowest revenue", "least revenue"]):
        return "lowest_lga"
    if any(k in text for k in ["business sector", "occupation", "top sector"]):
        return "top_sector"
    return "unknown"