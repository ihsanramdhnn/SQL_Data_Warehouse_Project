-- Cleaning bronze.crm_cust_info table --
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

-- Cleaning bronze.crm_prd_info table --
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

-- Cleaning bronze.crm_sales_details table --
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

-- Cleaning bronze.erp_cust_az12 --
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
