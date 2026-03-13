import random
import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime

fake = Faker()

# ----------------------------
# CONFIGURATION
# ----------------------------
NUM_LGAS = 20
NUM_TAXPAYERS = 20000
NUM_BUSINESSES = 5000
CURRENT_YEAR = 2024

# ----------------------------
# LGA DEFINITIONS (Real Lagos Inspired)
# ----------------------------
lga_names = [
    "Ikeja", "Lekki", "Surulere", "Alimosho", "Eti-Osa",
    "Yaba", "Victoria Island", "Ikorodu", "Epe", "Badagry",
    "Apapa", "Oshodi-Isolo", "Mushin", "Agege", "Kosofe",
    "Somolu", "Amuwo-Odofin", "Ifako-Ijaiye", "Shomolu", "Ajah"
]

# Income multipliers by LGA (to simulate wealth distribution)
lga_income_multiplier = {
    "Lekki": 4,
    "Victoria Island": 4,
    "Eti-Osa": 3,
    "Ikeja": 2.5,
    "Surulere": 2,
    "Yaba": 2,
    "Alimosho": 1,
    "Ikorodu": 1,
    "Agege": 1,
}

# ----------------------------
# TAX LOGIC
# ----------------------------
def calculate_tax(income):
    if income < 500000:
        return income * 0.05
    elif income < 2000000:
        return income * 0.10
    elif income < 10000000:
        return income * 0.18
    else:
        return income * 0.25

# ----------------------------
# GENERATE LGAS
# ----------------------------
lgas = []

for i, name in enumerate(lga_names):
    lgas.append({
        "id": i + 1,
        "name": name,
        "population": random.randint(150000, 1500000),
        "area_sq_km": round(random.uniform(20, 200), 2),
        "urbanization_level": random.choice(["High", "Medium", "Low"]),
        "average_property_value": random.randint(5_000_000, 200_000_000)
    })

lgas_df = pd.DataFrame(lgas)

# ----------------------------
# GENERATE TAXPAYERS
# ----------------------------
taxpayers = []

for i in range(NUM_TAXPAYERS):
    lga = random.choice(lga_names)
    multiplier = lga_income_multiplier.get(lga, 1.5)

    base_income = random.randint(200_000, 5_000_000)
    income = base_income * multiplier

    property_value = income * random.uniform(1.5, 4)

    taxpayers.append({
        "id": i + 1,
        "full_name": fake.name(),
        "age": random.randint(21, 70),
        "occupation": fake.job(),
        "lga": lga,
        "declared_income": round(income, 2),
        "property_value": round(property_value, 2),
        "business_owner": random.choice([True, False]),
        "compliance_score": round(random.uniform(40, 100), 2),
        "created_at": fake.date_this_decade()
    })

taxpayers_df = pd.DataFrame(taxpayers)


# GENERATE TAX RECORDS

tax_records = []

for taxpayer in taxpayers:
    income = taxpayer["declared_income"]
    expected_tax = calculate_tax(income)

    compliance_factor = taxpayer["compliance_score"] / 100
    tax_paid = expected_tax * compliance_factor

    tax_records.append({
        "taxpayer_id": taxpayer["id"],
        "tax_year": CURRENT_YEAR,
        "declared_income": income,
        "expected_tax": round(expected_tax, 2),
        "tax_paid": round(tax_paid, 2),
        "payment_status": (
            "Paid" if compliance_factor > 0.9 else
            "Partial" if compliance_factor > 0.5 else
            "Unpaid"
        )
    })

tax_records_df = pd.DataFrame(tax_records)

# ----------------------------
# GENERATE BUSINESSES
# ----------------------------
businesses = []

for i in range(NUM_BUSINESSES):
    lga = random.choice(lga_names)

    businesses.append({
        "id": i + 1,
        "business_name": fake.company(),
        "sector": random.choice(["Tech", "Retail", "Manufacturing", "Finance", "Real Estate"]),
        "lga": lga,
        "annual_revenue": random.randint(5_000_000, 500_000_000),
        "employee_count": random.randint(5, 500),
        "registered": random.choice([True, False])
    })

businesses_df = pd.DataFrame(businesses)

# ----------------------------
# GENERATE PROPERTIES
# ----------------------------
properties = []

for i in range(NUM_TAXPAYERS):
    properties.append({
        "id": i + 1,
        "owner_id": i + 1,
        "lga": taxpayers[i]["lga"],
        "property_type": random.choice(["Residential", "Commercial"]),
        "estimated_value": round(taxpayers[i]["property_value"], 2)
    })

properties_df = pd.DataFrame(properties)

# EXPORT TO CSV

lgas_df.to_csv("lgas.csv", index=False)
taxpayers_df.to_csv("taxpayers.csv", index=False)
tax_records_df.to_csv("tax_records.csv", index=False)
businesses_df.to_csv("businesses.csv", index=False)
properties_df.to_csv("properties.csv", index=False)

print(" Synthetic Lagos Revenue Dataset Generated Successfully!")