import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="lagos_revenue",
    user="iwebukeonyeamachi",
    password="Richiwin"
)

cur = conn.cursor()

def execute_sql(sql):
    cur.execute(sql)
    return cur.fetchall()

# TOTAL REVENUE
def total_revenue(lga=None):

    condition = ""
    if lga:
        condition = f"WHERE LOWER(lga)='{lga}'"

    taxpayer_sql = f"SELECT SUM(declared_income) FROM taxpayers {condition}"
    business_sql = f"SELECT SUM(annual_revenue) FROM businesses {condition}"
    property_sql = f"SELECT SUM(estimated_value) FROM properties {condition}"

    taxpayer = execute_sql(taxpayer_sql)[0][0] or 0
    business = execute_sql(business_sql)[0][0] or 0
    property = execute_sql(property_sql)[0][0] or 0

    return {
        "taxpayer_income": taxpayer,
        "business_income": business,
        "property_income": property,
        "total_revenue": taxpayer + business + property
    }


# TOP LGA
def top_lga():

    sql = """
    SELECT lga,
    SUM(declared_income) as revenue
    FROM taxpayers
    GROUP BY lga
    ORDER BY revenue DESC
    LIMIT 1
    """

    row = execute_sql(sql)[0]

    return {
        "lga": row[0],
        "revenue": float(row[1])
    }


# LOWEST LGA
def lowest_lga():

    sql = """
    SELECT lga,
    SUM(declared_income) as revenue
    FROM taxpayers
    GROUP BY lga
    ORDER BY revenue ASC
    LIMIT 1
    """

    row = execute_sql(sql)[0]

    return {
        "lga": row[0],
        "revenue": float(row[1])
    }


# TOP BUSINESS SECTORS
def top_sectors():

    sql = """
    SELECT occupation,
    SUM(declared_income) as total
    FROM taxpayers
    GROUP BY occupation
    ORDER BY total DESC
    LIMIT 5
    """

    rows = execute_sql(sql)

    data = []

    for occ, amount in rows:
        data.append({
            "occupation": occ,
            "revenue": float(amount)
        })

    return data


# TAX GAP
def tax_gap():

    sql = """
    SELECT t.full_name,
    SUM(tr.expected_tax - tr.tax_paid) AS unpaid_tax
    FROM taxpayers t
    JOIN tax_records tr
    ON t.id = tr.taxpayer_id
    WHERE tr.tax_paid < tr.expected_tax
    GROUP BY t.full_name
    ORDER BY unpaid_tax DESC
    LIMIT 5
    """

    rows = execute_sql(sql)

    results = []

    for name, amount in rows:
        results.append({
            "name": name,
            "unpaid_tax": float(amount)
        })

    return results