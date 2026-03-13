-- Drop tables if they exist
DROP TABLE IF EXISTS tax_records CASCADE;
DROP TABLE IF EXISTS properties CASCADE;
DROP TABLE IF EXISTS businesses CASCADE;
DROP TABLE IF EXISTS taxpayers CASCADE;

-- Taxpayers table
CREATE TABLE taxpayers (
    id SERIAL PRIMARY KEY,
    full_name VARCHAR(100),
    age INT,
    occupation VARCHAR(100),
    lga VARCHAR(100),
    declared_income NUMERIC,
    property_value NUMERIC,
    business_owner BOOLEAN,
    compliance_score NUMERIC,
    created_at TIMESTAMP
);

-- Properties table
CREATE TABLE properties (
    id SERIAL PRIMARY KEY,
    owner_id INT REFERENCES taxpayers(id),
    lga VARCHAR(100),
    property_type VARCHAR(50),
    estimated_value NUMERIC
);

-- Tax Records table
CREATE TABLE tax_records (
    id SERIAL PRIMARY KEY,
    taxpayer_id INT REFERENCES taxpayers(id),
    tax_year INT,
    declared_income NUMERIC,
    expected_tax NUMERIC,
    tax_paid NUMERIC,
    payment_status VARCHAR(50)
);

-- Businesses table
CREATE TABLE businesses (
    id SERIAL PRIMARY KEY,
    business_name VARCHAR(200),
    sector VARCHAR(100),
    lga VARCHAR(100),
    annual_revenue NUMERIC,
    employee_count INT,
    registered BOOLEAN
);

CREATE TABLE lagos_lgas (
    id SERIAL PRIMARY KEY,
    lga_name VARCHAR(100) NOT NULL UNIQUE,
    latitude DECIMAL(10, 8) NOT NULL,
    longitude DECIMAL(11, 8) NOT NULL
);