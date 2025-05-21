/*
===============================================================================
DDL Script: Gold Layer View Creation
===============================================================================
Purpose:
    This script defines views for the Gold layer of the data warehouse. 
    The Gold layer serves as the finalized presentation layer, structured 
    as a Star Schema with dimension and fact tables.

    Each view applies transformations and integrates data from the Silver layer 
    to deliver clean, enriched, and analysis-ready datasets.

Usage:
    - These views are intended for direct use in reporting and analytical queries.
===============================================================================
*/

-- =============================================================================
-- View Definition: gold.dim_customers
-- =============================================================================

create view gold.dim_customers as
SELECT row_number() OVER (ORDER BY cst_id) AS 
customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
la.cntry AS country, 
ci.cst_marital_status AS marital_status,
CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
ELSE COALESCE (ca.gen, 'n/a')
END AS gender,
ca.bdate AS birthdate,
ci.cst_create_date AS create_date
FROM SILVER.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca 
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

-- =============================================================================
-- View Definition: gold.dim_products
-- =============================================================================

create view gold.dim_products as 
select 
ROW_NUMBER() over (order by pn.prd_start_dt, pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as sub_category,
pc.maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where pn.prd_end_dt is null -- Filter out historical data

-- =============================================================================
-- View Definition: gold.fact_sales
-- =============================================================================

create view gold.fact_sales as
select 
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id
