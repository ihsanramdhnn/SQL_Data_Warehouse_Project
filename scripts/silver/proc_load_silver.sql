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

