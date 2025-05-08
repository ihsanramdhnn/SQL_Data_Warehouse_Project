/*
======================================================================================
Stored Procedure: Load Silver Layer (Source -> Bronze)
======================================================================================
Script Purpose:
This stored procedure loads data from the bronze layer into the silver schema. It performs a series of
ETL (Exract, Tranform, Load) process to populate 'silver' schema tables from 'bronze' schema.
The main operations include:
  - Truncate Silver Tables.
  - Insert Transformed and cleaned data from bronze into silver layer.
ETL processes:
  - Data Cleansing: Handles missing, null, or invalid values.
  - Data Standardization: Maps codes or inconsistent entries into standardized, descriptive formats.
  - Data Normalization: Ensures uniform structure and format of data for consistency across tables.
  - Derived Columns: Creates new columns based on transformations or calculations to enhance usability.
  - Data Enrichment: Enhances existing data by integrating additional relevant information or calculated fields.

Parameters:
   None
   This stored procedure does not accept any parameters or return any values.

Usage Example:
  EXEC silver.load_silver;
=====================================================================================
*/


create or alter procedure silver.load_silver as
begin
declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime
begin try
set @batch_start_time = getdate()
print '============================================'
print 'Load Silver Layer'
print '============================================'

print 'Loading silver.crm_cust_info'
print '--------------------------------------------'

set @start_time = getdate()
print '>> Truncating Table: silver.crm_cust_info'
truncate table silver.crm_cust_info
print '>> Inserting Data Into: silver.crm_cust_info'
insert into silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)
select
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname, -- Remove unwanted spaces
trim(cst_lastname) as cst_lastname, -- Remove unwanted spaces
case when upper(trim(cst_marital_status)) = 'M' then 'Maried'
	when upper(trim(cst_marital_status)) = 'S' then 'Single'
	else 'n/a'
end as cst_marital_status, -- Normalize marital status values to readable format and Handling missing data
case when upper(trim(cst_gndr)) = 'M' then 'Male'
	when upper(trim(cst_gndr)) = 'F' then 'Female'
	else 'n/a'
end as cst_gndr, -- Normalize gender values to readable format and Handling missing data
cst_create_date
from(
select
*,
row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info
where cst_id is not null
)t where flag_last = 1 -- Handling duplicates by select the most recent record per customer
set @end_time = getdate()
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds'
print '============================================'

print 'Loading silver.crm_prd_info'
print '--------------------------------------------'

set @start_time = getdate()
print '>> Truncating Table: silver.crm_prd_info'
truncate table silver.crm_prd_info
print '>> Inserting Data Into: silver.crm_prd_info'
insert into silver.crm_prd_info(
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)
SELECT
prd_id,
replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id, -- Extract category ID
SUBSTRING(prd_key,7,len(prd_key)) as prd_key,	   -- Extract product key
prd_nm,
isnull(prd_cost,0) as prd_cost, -- Handling NULL value to 0
case when upper(trim(prd_line)) = 'M' then 'Mountain'
	when upper(trim(prd_line)) = 'R' then 'Road'
	when upper(trim(prd_line)) = 'S' then 'Other Sales'
	when upper(trim(prd_line)) = 'T' then 'Touring'
else 'n/a'
end as prd_line, -- Mapping line code to descriptive values
cast (prd_start_dt as date) as prd_start_dt, -- Data Transformation (datetime -> date)
cast (lead(prd_start_dt) over (partition by prd_key ORDER BY prd_start_dt)-1 as date) 
as prd_end_dt -- Calculate end date as one day before the next start date
  FROM bronze.crm_prd_info
set @end_time = getdate()
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds'
print '============================================'

print 'Loading silver.crm_sales_details'
print '--------------------------------------------'

set @start_time = getdate()
print '>> Truncating Table: silver.crm_sales_details'
truncate table silver.crm_sales_details
print '>> Inserting Data Into: silver.crm_sales_details'
insert into silver.crm_sales_details (
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = 0 or len(sls_order_dt) !=8 then NULL
else cast(cast(sls_order_dt as varchar) as date)
end as sls_order_dt,
case when sls_ship_dt = 0 or len(sls_ship_dt) !=8 then NULL
else cast(cast(sls_ship_dt as varchar) as date)
end as sls_ship_dt,
case when sls_due_dt = 0 or len(sls_due_dt) !=8 then NULL
else cast(cast(sls_due_dt as varchar) as date)
end as sls_due_dt, -- Handling invalid data and casting it into date data
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
	then sls_quantity * abs(sls_price)
	else sls_sales
end as sls_sales, -- Handling invalid data by recalculate sales if original value is missing or incorrect
sls_quantity,
case when sls_price is null or sls_price <=0
	then abs(sls_sales) / nullif(sls_quantity,0)
	else sls_price
end as sls_price -- Handling invalid data by recalculate price if original value is missing or incorrect
from bronze.crm_sales_details
set @end_time = getdate()
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds'
print '============================================'

print 'Loading silver.erp_cust_az12'
print '--------------------------------------------'

set @start_time = getdate()
print '>> Truncating Table:	silver.erp_cust_az12'
truncate table silver.erp_cust_az12
print '>> Inserting Data Into: silver.erp_cust_az12'
insert into silver.erp_cust_az12 (
cid,
bdate,
gen)
SELECT 
case when cid like 'NAS%' then substring(cid,4,len(cid)) -- Remove 'NAS' prefix because it's irrelevant
	else cid
end as cid,
case when bdate > getdate() then null -- Set future bdate to NULL
else bdate
end as bdate,
case when upper(trim(gen)) in ('F', 'Female') then 'Female' -- Handling NULL and empty values and normalize gender values
	when upper (trim(gen)) in ('M', 'Male') then 'Male'
else 'n/a'
end as gen
from bronze.erp_cust_az12
set @end_time = getdate()
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds'
print '============================================'

print 'Loading silver.erp_loc_a101'
print '--------------------------------------------'

set @start_time = getdate()
print '>> Truncating Table: silver.erp_loc_a101'
truncate table silver.erp_loc_a101
print '>> Inserting Data Into silver.erp_loc_a101'
insert into silver.erp_loc_a101(cid, cntry)
select 
replace(cid,'-','') as cid, -- Remove '-' to connect the column
case when trim(cntry) = 'DE' then 'Germany' -- Normalize and handling missing or blank country values
	when trim(cntry) in ('US','USA') then 'United States'
	when trim(cntry) = '' or cntry is null then 'n/a'
	else trim(cntry)
end as cntry
from bronze.erp_loc_a101
set @end_time = getdate()
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds'
print '============================================'

print 'Loading silver.erp_px_cat_g1v2'
print '--------------------------------------------'

set @start_time = getdate()
print '>> Truncating Table: silver.erp_px_cat_g1v2'
truncate table silver.erp_px_cat_g1v2
print '>> Inserting Data Into: silver.erp_px_cat_g1v2'
insert into silver.erp_px_cat_g1v2
(id, cat, subcat, maintenance)
select 
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2
set @end_time = getdate()
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds'
print '============================================'

set @batch_end_time = getdate()
print '============================================'
print 'Loading Silver Layer is Completed'
print '   - Total Load Duration: ' +cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' Seconds'
print '============================================'

end try
begin catch
print '============================================'
print 'Error occurred during loading silver layer'
print 'Error Message' + error_message()
print 'Error Message' + cast(error_number() as nvarchar)
print 'Error Message' + cast(error_state() as nvarchar)
print '============================================'
end catch
end
