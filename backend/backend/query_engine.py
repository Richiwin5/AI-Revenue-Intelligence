# query_engine.py
import psycopg2
import os

# -------------------- DATABASE CONNECTION --------------------
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "lagos_revenue")
DB_USER = os.getenv("DB_USER", "iwebukeonyeamachi")
DB_PASS = os.getenv("DB_PASS", "Richiwin")

def get_db_connection():
    """Create and return a database connection"""
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def execute_sql(sql, params=None):
    """Execute SQL and return results"""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql, params or [])
        rows = cur.fetchall()
        conn.commit()
        return rows
    except Exception as e:
        print("SQL ERROR:", e)
        conn.rollback()
        return []
    finally:
        cur.close()
        conn.close()

def format_currency(val):
    return float(val) if val else 0

# -------------------- TAX GAP --------------------
def get_tax_gap(lga=None, limit=5):
    """Get top taxpayers with unpaid tax"""
    sql = f"""
        SELECT 
            t.full_name,
            SUM(tr.expected_tax - tr.tax_paid) AS unpaid_tax,
            tr.source AS source,
            COALESCE(t.lga, 'Unknown') AS location
        FROM taxpayers t
        JOIN tax_records tr ON t.id = tr.taxpayer_id
        WHERE tr.tax_paid < tr.expected_tax
        {"AND LOWER(t.lga) = LOWER(%s)" if lga else ""}
        GROUP BY t.full_name, tr.source, t.lga
        ORDER BY unpaid_tax DESC
        LIMIT {limit};
    """
    rows = execute_sql(sql, [lga] if lga else None)
    return [
        {
            "name": r[0],
            "unpaid_tax": float(r[1]) if r[1] else 0,
            "source": r[2],
            "location": r[3]
        } for r in rows
    ]

# -------------------- TOTAL REVENUE --------------------
def get_total_revenue(lga=None):
    """Get total revenue from taxpayers"""
    sql = "SELECT SUM(declared_income) FROM taxpayers"
    params = []
    if lga:
        sql += " WHERE LOWER(lga) = LOWER(%s)"
        params = [lga]
    total = execute_sql(sql, params)[0][0] or 0
    return {"total_revenue": format_currency(total)}

# -------------------- TOTAL TAX --------------------
def get_total_tax(lga=None):
    sql = "SELECT SUM(expected_tax) FROM tax_records tr JOIN taxpayers t ON t.id = tr.taxpayer_id"
    params = []
    if lga:
        sql += " WHERE LOWER(t.lga) = LOWER(%s)"
        params = [lga]
    total = execute_sql(sql, params)[0][0] or 0
    return {"total_tax": format_currency(total)}

# -------------------- TOP TAXPAYER --------------------
def get_top_taxpayer(lga=None):
    sql = """
        SELECT t.full_name, SUM(tr.expected_tax) AS total_tax
        FROM taxpayers t
        JOIN tax_records tr ON t.id = tr.taxpayer_id
    """
    params = []
    if lga:
        sql += " WHERE LOWER(t.lga) = LOWER(%s)"
        params = [lga]
    sql += " GROUP BY t.full_name ORDER BY total_tax DESC LIMIT 1"
    rows = execute_sql(sql, params)
    if not rows:
        return {"message": "No data found"}
    return {"name": rows[0][0], "total_tax": float(rows[0][1])}

# -------------------- AVERAGE COMPLIANCE --------------------
def get_average_compliance(lga=None):
    sql = "SELECT AVG(compliance_score) FROM taxpayers"
    params = []
    if lga:
        sql += " WHERE LOWER(lga) = LOWER(%s)"
        params = [lga]
    avg = execute_sql(sql, params)[0][0] or 0
    return {"average_compliance": round(float(avg), 2)}

# -------------------- TOP & LOWEST REVENUE LGA --------------------
def get_top_revenue_lga():
    sql = "SELECT lga, SUM(declared_income) FROM taxpayers GROUP BY lga ORDER BY SUM(declared_income) DESC LIMIT 1"
    row = execute_sql(sql)[0]
    return {"lga": row[0], "revenue": float(row[1])}

def get_lowest_revenue_lga():
    sql = "SELECT lga, SUM(declared_income) FROM taxpayers GROUP BY lga ORDER BY SUM(declared_income) ASC LIMIT 1"
    row = execute_sql(sql)[0]
    return {"lga": row[0], "revenue": float(row[1])}

# -------------------- TOP BUSINESS SECTORS --------------------
def get_top_sectors(limit=5):
    sql = f"""
        SELECT occupation, SUM(declared_income) AS revenue
        FROM taxpayers
        GROUP BY occupation
        ORDER BY revenue DESC
        LIMIT {limit}
    """
    rows = execute_sql(sql)
    return [{"sector": r[0], "revenue": float(r[1])} for r in rows]