import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="lagos_revenue",
    user="iwebukeonyeamachi",
    password="Richiwin"
)

cur = conn.cursor()


def execute_sql(sql, params=None):

    try:
        cur.execute(sql, params)
        return cur.fetchall()

    except Exception as e:
        print("Database Error:", e)
        conn.rollback()
        return []