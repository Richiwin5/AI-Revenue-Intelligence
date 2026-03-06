# backend/load_data.py

import psycopg2
import pandas as pd

# PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    database="lagos_revenue",
    user="iwebukeonyeamachi",
    password="Richiwin"  # use the password you set
)
cur = conn.cursor()

# Step 1: Execute schema
with open("schema.sql", "r") as f:
    cur.execute(f.read())
conn.commit()

# Step 2: Load CSVs from data folder
csv_files = {
    "taxpayers": "../data/taxpayers.csv",
    "businesses": "../data/businesses.csv",
    "properties": "../data/properties.csv",
    "tax_records": "../data/tax_records.csv"
}

for table, path in csv_files.items():
    df = pd.read_csv(path)
    print(f"Loading {len(df)} rows into {table}...")
    
    # Generate insert statements
    for i, row in df.iterrows():
        placeholders = ','.join(['%s'] * len(row))
        columns = ','.join(row.index)
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        cur.execute(sql, tuple(row))
    conn.commit()

cur.close()
conn.close()
print(" All CSVs loaded successfully!")