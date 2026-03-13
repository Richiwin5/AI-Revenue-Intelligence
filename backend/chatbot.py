# chatbot.py - Complete Revenue Intelligence Chatbot
import psycopg2
import os
import pandas as pd
import re
from datetime import datetime
from typing import Dict, List, Any, Optional
from functools import lru_cache
from difflib import get_close_matches

# -------------------- DATABASE CONNECTION --------------------
class DatabaseConnection:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.conn = None
        return cls._instance
    
    def get_connection(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                database=os.getenv("DB_NAME", "lagos_revenue"),
                user=os.getenv("DB_USER", "iwebukeonyeamachi"),
                password=os.getenv("DB_PASS", "Richiwin")
            )
        return self.conn
    
    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()

db = DatabaseConnection()

# -------------------- UTILITIES --------------------
def execute_sql(sql: str, params: tuple = None) -> List[tuple]:
    """Execute SQL safely"""
    conn = db.get_connection()
    try:
        with conn.cursor() as cur:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            try:
                return cur.fetchall()
            except:
                conn.commit()
                return []
    except Exception as e:
        print(f"SQL ERROR: {e}")
        conn.rollback()
        return []

def format_currency(val: float) -> str:
    """Format currency naturally with trillion support"""
    if not val:
        return "₦0"
    
    # Trillions (1000 billion = 1 trillion)
    if val >= 1_000_000_000_000:
        return f"₦{val/1_000_000_000_000:.2f} trillion"
    
    # Billions
    if val >= 1_000_000_000:
        return f"₦{val/1_000_000_000:.2f} billion"
    
    # Millions
    if val >= 1_000_000:
        return f"₦{val/1_000_000:.2f} million"
    
    # Thousands
    if val >= 1_000:
        return f"₦{val/1_000:.2f} thousand"
    
    return f"₦{val:,.0f}"

def format_percentage(val: float) -> str:
    return f"{val:.1f}%"

# -------------------- LGA LIST FROM CSV --------------------
@lru_cache(maxsize=1)
def get_all_lgas():
    """Get all LGAs from the CSV file"""
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(current_dir, "../data/lagos_lgas.csv")
        
        df = pd.read_csv(csv_path)
        
        # Try different possible column names
        for col in ['lga_name', 'LGA', 'name', 'lga']:
            if col in df.columns:
                lgas = df[col].tolist()
                break
        else:
            # Use first column
            lgas = df.iloc[:, 0].tolist()
        
        # Clean and return
        lgas = [str(lga).lower().strip() for lga in lgas if pd.notna(lga)]
        print(f"✅ Loaded {len(lgas)} LGAs")
        return lgas
        
    except Exception as e:
        print(f"⚠️  Error loading CSV: {e}")
        return []

# Load LGAs
LGA_LIST = get_all_lgas()

# Common variations and abbreviations (for quick lookup)
LGA_VARIATIONS = {
    'vi': 'victoria island',
    'v.i': 'victoria island',
    'v.i.': 'victoria island',
    'victoria': 'victoria island',
    'island': 'lagos island',
    'mainland': 'lagos mainland',
    'eti osa': 'eti-osa',
    'et-osa': 'eti-osa',
    'etiosa': 'eti-osa',
    'eti': 'eti-osa',
    'ajah': 'ajah',
    'lekki': 'lekki',
    'ikoyi': 'ikoyi',
    'surulere': 'surulere',
    'yaba': 'yaba',
    'apapa': 'apapa',
    'mushin': 'mushin',
    'agege': 'agege',
    'shomolu': 'shomolu',
    'somolu': 'shomolu',
    'kosofe': 'kosofe',
    'ikeja': 'ikeja',
    'ikorodu': 'ikorodu',
    'badagry': 'badagry',
    'alimosho': 'alimosho',
    'epe': 'epe',
    'ibeju': 'ibeju-lekki'
}

# Business sectors
BUSINESS_SECTORS = [
    'tech', 'technology', 'finance', 'real estate', 'retail', 
    'manufacturing', 'construction', 'healthcare', 'education',
    'agriculture', 'transportation', 'hospitality', 'entertainment'
]

# -------------------- SPELLING CORRECTION --------------------
def correct_lga_spelling(text: str) -> Optional[str]:
    """Try to correct misspelled LGA names"""
    text_lower = text.lower()
    
    # Check variations first
    for var, full in LGA_VARIATIONS.items():
        if var == text_lower or var in text_lower.split():
            return full
    
    # Try to find close matches
    words = text_lower.split()
    for word in words:
        if len(word) > 3:  # Only try to correct words with more than 3 letters
            matches = get_close_matches(word, LGA_LIST, n=1, cutoff=0.8)
            if matches:
                return matches[0]
    
    return None

# -------------------- GREETING DETECTION --------------------
def is_greeting(text: str) -> bool:
    """Check if the input is just a greeting"""
    text_lower = text.lower().strip()
    
    simple_greetings = [
        'hello', 'hi', 'hey', 'howdy', 'greetings', 
        'good morning', 'good afternoon', 'good evening',
        'morning', 'afternoon', 'evening', 'good day',
        'what\'s up', 'sup', 'how are you', 'how do you do',
        'hi there', 'hello there'
    ]
    
    if text_lower in simple_greetings:
        return True
    
    for greeting in simple_greetings:
        if text_lower.startswith(greeting) and len(text_lower.split()) <= 3:
            return True
    
    return False

def get_greeting_response(text: str) -> str:
    """Get appropriate greeting response"""
    text_lower = text.lower()
    
    if 'good morning' in text_lower:
        return "Good morning! How can I help you with Lagos revenue data today?"
    elif 'good afternoon' in text_lower:
        return "Good afternoon! What would you like to know about revenue, taxes, or businesses?"
    elif 'good evening' in text_lower:
        return "Good evening! I'm here to answer any questions about Lagos revenue."
    elif 'good day' in text_lower:
        return "Good day! How can I assist you with revenue information?"
    else:
        import random
        responses = [
            "Hello! How can I assist you with revenue data today?",
            "Hi there! Ask me about revenue, taxes, defaulters, or business sectors.",
            "Hey! I can help with revenue comparisons, top defaulters, and more.",
            "Hello! What would you like to know about Lagos revenue?"
        ]
        return random.choice(responses)

# -------------------- ACCURATE INTENT DETECTION --------------------
def extract_all_lgas(text: str) -> List[str]:
    """Extract ANY LGAs mentioned with spelling correction"""
    text_lower = text.lower()
    found = []
    
    # SPECIAL CASE: If they ask about "Lagos" or "Lagos State", return empty list (meaning overall)
    if ('lagos state' in text_lower or 
        text_lower.strip() == 'lagos' or 
        text_lower.strip() == 'total revenue' or
        'state' in text_lower):
        return []  # Empty list means overall Lagos State
    
    # Check variations first
    for var, full in LGA_VARIATIONS.items():
        if var in text_lower.split() or f" {var} " in f" {text_lower} ":
            if full not in found:
                found.append(full)
    
    # Check main LGA list for direct matches
    for lga in LGA_LIST:
        if lga in text_lower:
            if lga not in found:
                found.append(lga)
    
    # If still no matches, try spelling correction on each word
    if not found:
        words = text_lower.split()
        for word in words:
            corrected = correct_lga_spelling(word)
            if corrected and corrected not in found:
                found.append(corrected)
    
    return found

def detect_intent(text: str) -> Dict[str, Any]:
    """Accurately detect what the user wants"""
    text_lower = text.lower()
    
    # Check if this is a total revenue query for Lagos State
    is_total_revenue = (
        ('total' in text_lower or 'overall' in text_lower) and 
        ('revenue' in text_lower or 'money' in text_lower or 'income' in text_lower) and
        (len(extract_all_lgas(text)) == 0 or 'lagos' in text_lower or 'state' in text_lower)
    )
    
    if is_total_revenue:
        return {'intent': 'total_revenue_state', 'confidence': 1.0}
    
    # Priority 1: Business sector questions
    if any(word in text_lower for word in ['sector', 'industry', 'business sector']):
        if 'top' in text_lower or 'highest' in text_lower or 'best' in text_lower:
            return {'intent': 'top_sector', 'confidence': 1.0}
        return {'intent': 'sector_info', 'confidence': 0.9}
    
    # Priority 2: Tax defaulter questions
    if any(word in text_lower for word in ['defaulter', 'debtor', 'owe', 'owing', 'debt', 'unpaid', 'default']):
        return {'intent': 'tax_defaulters', 'confidence': 1.0}
    
    # Priority 3: Top taxpayer questions
    if ('top' in text_lower or 'highest' in text_lower) and ('taxpayer' in text_lower or 'tax payer' in text_lower):
        return {'intent': 'top_taxpayers', 'confidence': 1.0}
    
    # Priority 4: Property questions
    if any(word in text_lower for word in ['property', 'properties', 'real estate']):
        if 'top' in text_lower or 'most valuable' in text_lower or 'highest value' in text_lower:
            return {'intent': 'top_properties', 'confidence': 1.0}
        return {'intent': 'property_info', 'confidence': 0.9}
    
    # Priority 5: Compliance questions
    if any(word in text_lower for word in ['compliance', 'compliant', 'compliance rate']):
        return {'intent': 'compliance', 'confidence': 1.0}
    
    # Priority 6: Comparison questions
    if any(word in text_lower for word in ['difference', 'compare', 'versus', 'vs', 'between']):
        return {'intent': 'comparison', 'confidence': 1.0}
    
    # Priority 7: Revenue questions
    if any(word in text_lower for word in ['revenue', 'money', 'income', 'generate', 'collection']):
        # Check if it's asking for a specific LGA
        lgas = extract_all_lgas(text)
        if lgas:
            return {'intent': 'revenue_lga', 'confidence': 0.9}
        else:
            return {'intent': 'total_revenue_state', 'confidence': 0.9}
    
    # Default - ask for clarification
    return {'intent': 'unknown', 'confidence': 0.1}

# -------------------- DATA QUERIES --------------------
# ========== REVENUE QUERIES (includes ALL tables) ==========
@lru_cache(maxsize=128)
def get_lga_revenue(lga: str) -> float:
    """
    Calculate TOTAL revenue for a specific LGA from ALL sources:
    - Taxpayer declared income
    - Business annual revenue  
    - Property estimated value
    """
    sql = f"""
        SELECT 
            COALESCE((SELECT SUM(declared_income) FROM taxpayers WHERE LOWER(lga) = '{lga.lower()}'), 0) +
            COALESCE((SELECT SUM(annual_revenue) FROM businesses WHERE LOWER(lga) = '{lga.lower()}'), 0) +
            COALESCE((SELECT SUM(estimated_value) FROM properties WHERE LOWER(lga) = '{lga.lower()}'), 0)
        as total_revenue
    """
    result = execute_sql(sql)
    return result[0][0] if result else 0

@lru_cache(maxsize=128)
def get_total_revenue() -> float:
    """
    Calculate TOTAL revenue for Lagos State from ALL sources:
    - Taxpayer declared income
    - Business annual revenue
    - Property estimated value
    """
    sql = """
        SELECT 
            COALESCE((SELECT SUM(declared_income) FROM taxpayers), 0) +
            COALESCE((SELECT SUM(annual_revenue) FROM businesses), 0) +
            COALESCE((SELECT SUM(estimated_value) FROM properties), 0)
        as total_revenue
    """
    result = execute_sql(sql)
    return result[0][0] if result else 0

@lru_cache(maxsize=128)
def get_revenue_breakdown(lga: str = None) -> Dict[str, float]:
    """
    Get breakdown of revenue by source for a specific LGA or overall
    """
    if lga:
        lga_condition = f"WHERE LOWER(lga) = '{lga.lower()}'"
    else:
        lga_condition = ""
    
    sql = f"""
        SELECT 
            COALESCE((SELECT SUM(declared_income) FROM taxpayers {lga_condition}), 0) as taxpayer_income,
            COALESCE((SELECT SUM(annual_revenue) FROM businesses {lga_condition}), 0) as business_revenue,
            COALESCE((SELECT SUM(estimated_value) FROM properties {lga_condition}), 0) as property_value
    """
    result = execute_sql(sql)[0]
    
    return {
        'taxpayer_income': result[0],
        'business_revenue': result[1],
        'property_value': result[2],
        'total': result[0] + result[1] + result[2]
    }

# ========== SECTOR QUERIES ==========
@lru_cache(maxsize=128)
def get_top_sector_global() -> Dict:
    """Get top business sector overall"""
    sql = """
        SELECT sector, SUM(annual_revenue) as total
        FROM businesses
        WHERE sector IS NOT NULL
        GROUP BY sector
        ORDER BY total DESC
        LIMIT 1
    """
    result = execute_sql(sql)
    if result:
        return {'sector': result[0][0], 'revenue': result[0][1]}
    return {'sector': 'Unknown', 'revenue': 0}

@lru_cache(maxsize=128)
def get_top_sector_in_lga(lga: str) -> Optional[Dict]:
    """Get top business sector in a specific LGA"""
    sql = f"""
        SELECT sector, SUM(annual_revenue) as total
        FROM businesses
        WHERE LOWER(lga) = '{lga.lower()}' AND sector IS NOT NULL
        GROUP BY sector
        ORDER BY total DESC
        LIMIT 1
    """
    result = execute_sql(sql)
    if result:
        return {'sector': result[0][0], 'revenue': result[0][1], 'lga': lga}
    return None

@lru_cache(maxsize=128)
def get_all_sectors_in_lga(lga: str) -> List[Dict]:
    """Get all sectors in a specific LGA with their revenue"""
    sql = f"""
        SELECT sector, SUM(annual_revenue) as total
        FROM businesses
        WHERE LOWER(lga) = '{lga.lower()}' AND sector IS NOT NULL
        GROUP BY sector
        ORDER BY total DESC
    """
    results = execute_sql(sql)
    return [{'sector': r[0], 'revenue': r[1]} for r in results]

@lru_cache(maxsize=128)
def get_sector_revenue_global(sector: str) -> float:
    """Get revenue for a specific sector across all LGAs"""
    sql = f"""
        SELECT COALESCE(SUM(annual_revenue), 0)
        FROM businesses
        WHERE LOWER(sector) LIKE '%{sector.lower()}%'
    """
    result = execute_sql(sql)
    return result[0][0] if result else 0

@lru_cache(maxsize=128)
def get_sector_revenue_in_lga(sector: str, lga: str) -> float:
    """Get revenue for a specific sector in a specific LGA"""
    sql = f"""
        SELECT COALESCE(SUM(annual_revenue), 0)
        FROM businesses
        WHERE LOWER(sector) LIKE '%{sector.lower()}%' 
        AND LOWER(lga) = '{lga.lower()}'
    """
    result = execute_sql(sql)
    return result[0][0] if result else 0

@lru_cache(maxsize=128)
def get_all_sectors_global() -> List[Dict]:
    """Get all sectors with their revenue across all LGAs"""
    sql = """
        SELECT sector, SUM(annual_revenue) as total
        FROM businesses
        WHERE sector IS NOT NULL
        GROUP BY sector
        ORDER BY total DESC
    """
    results = execute_sql(sql)
    return [{'sector': r[0], 'revenue': r[1]} for r in results]

# ========== TAX QUERIES ==========
@lru_cache(maxsize=128)
def get_tax_defaulters(lga: str = None, limit: int = 5) -> List[Dict]:
    """Get tax defaulters"""
    if lga:
        lga_condition = f"AND LOWER(t.lga) = '{lga.lower()}'"
    else:
        lga_condition = ""
    
    sql = f"""
        SELECT 
            t.full_name,
            t.lga,
            tr.expected_tax - tr.tax_paid as unpaid
        FROM tax_records tr
        JOIN taxpayers t ON t.id = tr.taxpayer_id
        WHERE tr.tax_paid < tr.expected_tax {lga_condition}
        ORDER BY unpaid DESC
        LIMIT {limit}
    """
    
    results = execute_sql(sql)
    return [{'name': r[0], 'lga': r[1], 'unpaid': r[2]} for r in results]

@lru_cache(maxsize=128)
def get_top_taxpayers(lga: str = None, limit: int = 5) -> List[Dict]:
    """Get top taxpayers"""
    if lga:
        lga_condition = f"AND LOWER(t.lga) = '{lga.lower()}'"
    else:
        lga_condition = ""
    
    sql = f"""
        SELECT 
            t.full_name,
            t.lga,
            SUM(tr.tax_paid) as total_paid
        FROM tax_records tr
        JOIN taxpayers t ON t.id = tr.taxpayer_id
        WHERE 1=1 {lga_condition}
        GROUP BY t.full_name, t.lga
        ORDER BY total_paid DESC
        LIMIT {limit}
    """
    
    results = execute_sql(sql)
    return [{'name': r[0], 'lga': r[1], 'paid': r[2]} for r in results]

# ========== PROPERTY QUERIES ==========
@lru_cache(maxsize=128)
def get_top_properties(lga: str = None, limit: int = 5) -> List[Dict]:
    """Get top properties by value"""
    if lga:
        lga_condition = f"WHERE LOWER(p.lga) = '{lga.lower()}'"
    else:
        lga_condition = ""
    
    sql = f"""
        SELECT 
            p.property_type,
            p.lga,
            p.estimated_value,
            t.full_name as owner
        FROM properties p
        LEFT JOIN taxpayers t ON t.id = p.owner_id
        {lga_condition}
        ORDER BY p.estimated_value DESC
        LIMIT {limit}
    """
    
    results = execute_sql(sql)
    return [{'type': r[0], 'lga': r[1], 'value': r[2], 'owner': r[3] or 'Unknown'} for r in results]

# ========== COMPLIANCE QUERIES ==========
@lru_cache(maxsize=128)
def get_compliance_rate(lga: str = None) -> Dict:
    """Get compliance rate"""
    if lga:
        lga_condition = f"WHERE LOWER(lga) = '{lga.lower()}'"
    else:
        lga_condition = ""
    
    sql = f"""
        SELECT 
            AVG(compliance_score) as avg_score,
            COUNT(*) as total,
            SUM(CASE WHEN compliance_score >= 70 THEN 1 ELSE 0 END) as compliant
        FROM taxpayers
        {lga_condition}
    """
    
    result = execute_sql(sql)[0]
    total = result[1] or 0
    compliant = result[2] or 0
    
    return {
        'avg_score': result[0] or 0,
        'total': total,
        'compliant': compliant,
        'rate': (compliant / total * 100) if total > 0 else 0
    }

# -------------------- ACCURATE RESPONSE GENERATOR --------------------
def generate_response(question: str, lgas: List[str], intent_info: Dict) -> str:
    """Generate accurate response based on intent"""
    
    intent = intent_info['intent']
    target_lga = lgas[0] if lgas else None
    
    # ========== UNKNOWN INTENT - ASK FOR CLARIFICATION ==========
    if intent == 'unknown':
        return ("I'm not sure I understood your question. Could you please rephrase?\n\n"
                "You can ask about:\n"
                "• Revenue (e.g., 'total revenue in agege')\n"
                "• Tax defaulters (e.g., 'who owes tax in lekki')\n"
                "• Top taxpayers (e.g., 'top taxpayers in ikeja')\n"
                "• Business sectors (e.g., 'top business sector')\n"
                "• Properties (e.g., 'most valuable properties in vi')")
    
    # ========== TOTAL REVENUE FOR LAGOS STATE ==========
    if intent == 'total_revenue_state':
        # Get detailed breakdown for Lagos State
        breakdown = get_revenue_breakdown()
        return (f"💰 **Total Revenue for Lagos State:** {format_currency(breakdown['total'])}\n\n"
                f"📊 **Breakdown by source:**\n"
                f"• Taxpayer Income: {format_currency(breakdown['taxpayer_income'])}\n"
                f"• Business Revenue: {format_currency(breakdown['business_revenue'])}\n"
                f"• Property Value: {format_currency(breakdown['property_value'])}")
    
    # ========== REVENUE FOR SPECIFIC LGA ==========
    elif intent == 'revenue_lga' and target_lga:
        # Get detailed breakdown for the LGA
        breakdown = get_revenue_breakdown(target_lga)
        return (f"💰 **Total Revenue for {target_lga.title()}:** {format_currency(breakdown['total'])}\n\n"
                f"📊 **Breakdown by source:**\n"
                f"• Taxpayer Income: {format_currency(breakdown['taxpayer_income'])}\n"
                f"• Business Revenue: {format_currency(breakdown['business_revenue'])}\n"
                f"• Property Value: {format_currency(breakdown['property_value'])}")
    
    # ========== SECTOR QUESTIONS ==========
    elif intent == 'top_sector':
        if target_lga:
            # Get top sector in specific LGA
            top_sector = get_top_sector_in_lga(target_lga)
            if top_sector:
                return f"🏭 The top business sector in **{target_lga.title()}** is **{top_sector['sector']}** generating {format_currency(top_sector['revenue'])} in revenue."
            else:
                return f"No business sector data found for {target_lga.title()}."
        else:
            # Get top sector globally
            top = get_top_sector_global()
            return f"🏭 The top business sector overall is **{top['sector']}** generating {format_currency(top['revenue'])} in revenue."
    
    elif intent == 'sector_info':
        # Check if a specific sector was mentioned
        specific_sector = None
        for sector in BUSINESS_SECTORS:
            if sector in question.lower():
                specific_sector = sector
                break
        
        if specific_sector and target_lga:
            # Get specific sector in specific LGA
            revenue = get_sector_revenue_in_lga(specific_sector, target_lga)
            return f"📊 The {specific_sector.title()} sector in {target_lga.title()} generates {format_currency(revenue)} in revenue."
        
        elif specific_sector:
            # Get specific sector globally
            revenue = get_sector_revenue_global(specific_sector)
            return f"📊 The {specific_sector.title()} sector generates {format_currency(revenue)} in revenue across Lagos State."
        
        elif target_lga:
            # Show all sectors in specific LGA
            sectors = get_all_sectors_in_lga(target_lga)
            if sectors:
                response = f"📊 **Business Sectors in {target_lga.title()}:**\n"
                for s in sectors[:5]:
                    response += f"• {s['sector']}: {format_currency(s['revenue'])}\n"
                return response.strip()
            else:
                return f"No business sector data found for {target_lga.title()}."
        else:
            # Show all sectors globally
            sectors = get_all_sectors_global()
            response = "📊 **Business Sectors by Revenue:**\n"
            for s in sectors[:5]:
                response += f"• {s['sector']}: {format_currency(s['revenue'])}\n"
            return response.strip()
    
    # ========== TAX DEFAULTER QUESTIONS ==========
    elif intent == 'tax_defaulters':
        defaulters = get_tax_defaulters(target_lga)
        
        if not defaulters:
            location = target_lga.title() if target_lga else "Lagos State"
            return f"✅ No tax defaulters found in {location}!"
        
        response = f"⚠️ **Top Tax Defaulters**"
        if target_lga:
            response += f" in {target_lga.title()}"
        response += ":\n"
        
        for d in defaulters:
            response += f"• {d['name']}: owes {format_currency(d['unpaid'])}\n"
        return response.strip()
    
    # ========== TOP TAXPAYER QUESTIONS ==========
    elif intent == 'top_taxpayers':
        taxpayers = get_top_taxpayers(target_lga)
        
        if not taxpayers:
            return "No taxpayer data found."
        
        response = f"🏆 **Top Taxpayers**"
        if target_lga:
            response += f" in {target_lga.title()}"
        response += ":\n"
        
        for i, tp in enumerate(taxpayers, 1):
            response += f"{i}. {tp['name']}: paid {format_currency(tp['paid'])}\n"
        return response.strip()
    
    # ========== PROPERTY QUESTIONS ==========
    elif intent == 'top_properties':
        properties = get_top_properties(target_lga)
        
        if not properties:
            location = target_lga.title() if target_lga else "Lagos State"
            return f"No property data found for {location}."
        
        response = f"🏠 **Most Valuable Properties**"
        if target_lga:
            response += f" in {target_lga.title()}"
        response += ":\n"
        
        for i, prop in enumerate(properties, 1):
            response += f"{i}. {prop['type']} in {prop['lga']}: {format_currency(prop['value'])} (Owner: {prop['owner']})\n"
        return response.strip()
    
    # ========== COMPLIANCE QUESTIONS ==========
    elif intent == 'compliance':
        comp = get_compliance_rate(target_lga)
        location = target_lga.title() if target_lga else "Lagos State"
        
        return f"✅ **Tax Compliance in {location}:**\n• Compliance Rate: {format_percentage(comp['rate'])}\n• Average Score: {comp['avg_score']:.1f}%\n• Compliant Taxpayers: {comp['compliant']} out of {comp['total']}"
    
    # ========== COMPARISON QUESTIONS ==========
    elif intent == 'comparison':
        if len(lgas) >= 2:
            # Compare two LGAs
            lga1, lga2 = lgas[0], lgas[1]
            rev1 = get_lga_revenue(lga1)
            rev2 = get_lga_revenue(lga2)
            
            if rev1 > rev2:
                diff = rev1 - rev2
                percent = (diff / rev2) * 100
                return f"📊 **Comparison:** {lga1.title()} ({format_currency(rev1)}) generates {format_currency(diff)} more than {lga2.title()} ({format_currency(rev2)}). That's {percent:.1f}% higher."
            else:
                diff = rev2 - rev1
                percent = (diff / rev1) * 100
                return f"📊 **Comparison:** {lga2.title()} ({format_currency(rev2)}) generates {format_currency(diff)} more than {lga1.title()} ({format_currency(rev1)}). That's {percent:.1f}% higher."
        elif target_lga:
            # Compare with average
            rev = get_lga_revenue(target_lga)
            total = get_total_revenue()
            avg = total / len(LGA_LIST) if LGA_LIST else 0
            
            if rev > avg:
                return f"📊 {target_lga.title()} ({format_currency(rev)}) is {format_currency(rev - avg)} above the state average of {format_currency(avg)}."
            else:
                return f"📊 {target_lga.title()} ({format_currency(rev)}) is {format_currency(avg - rev)} below the state average of {format_currency(avg)}."
        else:
            return "Which LGAs would you like me to compare? Please specify two LGAs."
    
    # Default fallback
    return "I'm not sure I understood. Please ask about revenue, tax defaulters, top taxpayers, business sectors, or properties."

# -------------------- CONVERSATION MEMORY --------------------
class ConversationMemory:
    def __init__(self):
        self.history = []
    
    def add(self, question: str, lgas: List[str], intent: str):
        self.history.append({
            'question': question,
            'lgas': lgas,
            'intent': intent,
            'time': datetime.now()
        })
        if len(self.history) > 10:
            self.history = self.history[-10:]
    
    def get_last_lgas(self) -> List[str]:
        if self.history:
            return self.history[-1].get('lgas', [])
        return []
    
    def get_last_intent(self) -> Optional[str]:
        if self.history:
            return self.history[-1].get('intent')
        return None

# -------------------- MAIN CHATBOT --------------------
class RevenueChatbot:
    def __init__(self):
        self.memory = ConversationMemory()
        print(f"✅ Chatbot initialized with {len(LGA_LIST)} LGAs")
    
    def ask(self, question: str) -> str:
        """Process ANY question accurately"""
        
        # Check for greeting
        if is_greeting(question):
            return get_greeting_response(question)
        
        # Extract LGAs (with spelling correction)
        lgas = extract_all_lgas(question)
        
        # Detect intent
        intent_info = detect_intent(question)
        
        # Use memory only if:
        # 1. No LGAs found in current question
        # 2. Not asking for total revenue
        # 3. Intent matches
        if (not lgas and 
            intent_info['intent'] != 'total_revenue_state' and 
            intent_info['intent'] != 'unknown' and
            self.memory.get_last_lgas() and 
            intent_info['intent'] == self.memory.get_last_intent()):
            lgas = self.memory.get_last_lgas()
        
        # Generate response
        response = generate_response(question, lgas, intent_info)
        
        # Store in memory (but don't store total revenue or unknown queries as context)
        if intent_info['intent'] not in ['total_revenue_state', 'unknown']:
            self.memory.add(question, lgas, intent_info['intent'])
        
        return response
    
    def chat(self):
        """Interactive chat"""
        print("\n" + "="*60)
        print("🏛️  LAGOS REVENUE INTELLIGENCE CHATBOT")
        print("="*60)
        print("\nAsk me ANYTHING about Lagos revenue!")
        print("\nExamples:")
        print("  • 'good morning'")
        print("  • 'total revenue'")
        print("  • 'total money in epe'")
        print("  • 'top defaulters in shomolu'")
        print("  • 'what is the difference between badagry and mushin'")
        print("  • 'top business sector in lekki'")
        print("  • 'top taxpayers in ikeja'")
        print("  • 'most valuable properties in vi'")
        print("\nI can understand misspellings like 'et-osa' for 'eti-osa'!")
        print("\nType 'exit' to quit\n")
        
        while True:
            try:
                user_input = input("\nYou: ").strip()
                
                if user_input.lower() in ['exit', 'quit', 'bye', 'thanks', 'thank you']:
                    print("\nGoodbye! 👋")
                    break
                
                if not user_input:
                    continue
                
                response = self.ask(user_input)
                print(f"Bot: {response}")
                
            except KeyboardInterrupt:
                print("\n\nGoodbye! 👋")
                break
            except Exception as e:
                print(f"Error: {e}")

# -------------------- RUN --------------------
if __name__ == "__main__":
    bot = RevenueChatbot()
    bot.chat()
    db.close()










































































































































# # chatbot.py
# import psycopg2
# import os
# from intent_engine import parse_question
# from functools import lru_cache

# # -------------------- DATABASE CONNECTION --------------------
# conn = psycopg2.connect(
#     host=os.getenv("DB_HOST", "localhost"),
#     database=os.getenv("DB_NAME", "lagos_revenue"),
#     user=os.getenv("DB_USER", "iwebukeonyeamachi"),
#     password=os.getenv("DB_PASS", "Richiwin")
# )
# cur = conn.cursor()

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
#     if not val:
#         return "₦0.00"
#     return f"₦{float(val):,.2f}"

# # -------------------- TOTALS --------------------
# @lru_cache(maxsize=None)
# def get_total_revenue(lga=None):
#     """Total revenue from all tables"""
#     lga_condition = ""
#     if lga:
#         lga_condition = f"WHERE LOWER(lga) = '{lga}'"
#     # Sum taxpayers
#     tax_sql = f"SELECT SUM(declared_income) FROM taxpayers {lga_condition};"
#     taxpayer_sum = execute_sql(tax_sql)[0][0] or 0
#     # Sum businesses
#     bus_sql = f"SELECT SUM(annual_revenue) FROM businesses {lga_condition};"
#     business_sum = execute_sql(bus_sql)[0][0] or 0
#     # Sum properties
#     prop_sql = f"SELECT SUM(estimated_value) FROM properties {lga_condition};"
#     property_sum = execute_sql(prop_sql)[0][0] or 0

#     return taxpayer_sum + business_sum + property_sum

# def get_top_lga():
#     sql = """
#         SELECT lga, SUM(declared_income) + SUM(annual_revenue) + SUM(estimated_value) AS total
#         FROM (
#             SELECT lga, declared_income, 0 AS annual_revenue, 0 AS estimated_value FROM taxpayers
#             UNION ALL
#             SELECT lga, 0, annual_revenue, 0 FROM businesses
#             UNION ALL
#             SELECT lga, 0, 0, estimated_value FROM properties
#         ) t
#         GROUP BY lga
#         ORDER BY total DESC
#         LIMIT 1;
#     """
#     row = execute_sql(sql)
#     if row:
#         return {"lga": row[0][0], "total": row[0][1]}
#     return None

# def get_lowest_lga():
#     sql = """
#         SELECT lga, SUM(declared_income) + SUM(annual_revenue) + SUM(estimated_value) AS total
#         FROM (
#             SELECT lga, declared_income, 0 AS annual_revenue, 0 AS estimated_value FROM taxpayers
#             UNION ALL
#             SELECT lga, 0, annual_revenue, 0 FROM businesses
#             UNION ALL
#             SELECT lga, 0, 0, estimated_value FROM properties
#         ) t
#         GROUP BY lga
#         ORDER BY total ASC
#         LIMIT 1;
#     """
#     row = execute_sql(sql)
#     if row:
#         return {"lga": row[0][0], "total": row[0][1]}
#     return None

# def get_top_sector():
#     sql = """
#         SELECT occupation, SUM(declared_income) AS total
#         FROM taxpayers
#         GROUP BY occupation
#         ORDER BY total DESC
#         LIMIT 1;
#     """
#     row = execute_sql(sql)
#     if row:
#         return {"occupation": row[0][0], "total": row[0][1]}
#     return None

# def get_taxpayer_count(lga=None):
#     lga_condition = ""
#     if lga:
#         lga_condition = f"WHERE LOWER(lga) = '{lga}'"
#     sql = f"SELECT COUNT(*) FROM taxpayers {lga_condition};"
#     return execute_sql(sql)[0][0] or 0

# def get_total_paye(lga=None):
#     lga_condition = ""
#     if lga:
#         lga_condition = f"AND LOWER(t.lga) = '{lga}'"
#     sql = f"""
#         SELECT SUM(tr.tax_paid) 
#         FROM tax_records tr 
#         JOIN taxpayers t ON t.id = tr.taxpayer_id
#         WHERE tr.tax_paid > 0 {lga_condition};
#     """
#     return execute_sql(sql)[0][0] or 0

# # -------------------- MAIN ASK FUNCTION --------------------
# def ask_question(question):
#     parsed = parse_question(question)
#     intent = parsed.get("intent")
#     lga = parsed.get("lga")

#     if intent == "total_revenue":
#         total = get_total_revenue(lga)
#         if lga:
#             return f"Total revenue in {lga.title()}: {format_currency(total)}"
#         return f"Total revenue in Lagos: {format_currency(total)}"

#     elif intent == "top_lga":
#         top = get_top_lga()
#         if top:
#             return f"Top LGA by revenue: {top['lga'].title()} ({format_currency(top['total'])})"
#         return "Could not find top LGA."

#     elif intent == "lowest_lga":
#         low = get_lowest_lga()
#         if low:
#             return f"Lowest LGA by revenue: {low['lga'].title()} ({format_currency(low['total'])})"
#         return "Could not find lowest LGA."

#     elif intent == "top_sector":
#         top = get_top_sector()
#         if top:
#             return f"Top business sector: {top['occupation']} ({format_currency(top['total'])})"
#         return "Could not find top sector."

#     elif intent == "taxpayer_data":
#         count = get_taxpayer_count(lga)
#         if lga:
#             return f"Taxpayer count in {lga.title()}: {count}"
#         return f"Total taxpayers in Lagos: {count}"

#     elif intent == "business_revenue":
#         total = get_total_revenue(lga)
#         return f"Total business revenue{f' in {lga.title()}' if lga else ''}: {format_currency(total)}"

#     elif intent == "total_tax":
#         lga_condition = f"WHERE LOWER(t.lga) = '{lga}'" if lga else ""
#         sql = f"SELECT SUM(tr.expected_tax) FROM tax_records tr JOIN taxpayers t ON t.id = tr.taxpayer_id {lga_condition};"
#         total = execute_sql(sql)[0][0] or 0
#         return f"Total tax collected{f' in {lga.title()}' if lga else ''}: {format_currency(total)}"

#     elif intent == "tax_gap":
#         lga_condition = f"AND LOWER(t.lga) = '{lga}'" if lga else ""
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

#     elif intent == "compliance":
#         lga_condition = f"WHERE LOWER(lga) = '{lga}'" if lga else ""
#         sql = f"SELECT AVG(compliance_score) FROM taxpayers {lga_condition};"
#         result = execute_sql(sql)[0][0] or 0
#         return f"Average compliance{f' in {lga.title()}' if lga else ''}: {float(result):.2f}%"

#     return "I couldn't understand the question. Try asking about revenue, taxpayers, businesses, or properties."

# # -------------------- CHAT LOOP --------------------
# if __name__ == "__main__":
#     print("Lagos Revenue AI Chatbot")
#     print("Type exit to quit\n")

#     while True:
#         user_input = input("You: ")
#         if user_input.lower() in ["exit", "quit"]:
#             break
#         response = ask_question(user_input)
#         print("Bot:", response)

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

