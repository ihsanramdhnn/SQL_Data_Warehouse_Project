/*
===============================================================================
Quality Validation Script
===============================================================================
Objective:
    This script conducts data quality validations to ensure the reliability, 
    consistency, and integrity of the Gold Layer. The checks are designed to:
    - Confirm uniqueness of surrogate keys in dimension tables.
    - Verify referential integrity between fact and dimension tables.
    - Validate proper linkage across the data model for reporting accuracy.

Usage Guidelines:
    - Review any anomalies returned by these queries and address them accordingly.
===============================================================================
*/

-- ====================================================================
-- Validation: gold.dim_customers
-- ====================================================================
-- Ensure surrogate keys in gold.dim_customers are unique
-- Expected Result: No rows returned (no duplicates)
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1

-- ====================================================================
-- Validation: gold.dim_products
-- ====================================================================
-- Ensure surrogate keys in gold.dim_products are unique
-- Expected Result: No rows returned (no duplicates)
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1

-- ====================================================================
-- Validation: gold.fact_sales
-- ====================================================================
-- Check referential integrity between fact table and dimension tables
-- Expected Result: No rows where keys are unmatched
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
  ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
  ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL
