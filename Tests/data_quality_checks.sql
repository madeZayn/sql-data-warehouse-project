
--Table 1: bronze.crm_cust_info

select cst_id,count(*) as RecordCount from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null

--Primary key check for uniqueness
select * from (
select *, row_number() over(partition by cst_id order by cst_create_date desc) as last_flag
from bronze.crm_cust_info) t 
where last_flag = 1

--Check for unwanted Spaces
select cst_firstname from 
bronze.crm_cust_info
where cst_firstname <> TRIM(cst_firstname)

select cst_gndr from 
bronze.crm_cust_info
where cst_gndr <> TRIM(cst_gndr)

select DISTINCT cst_gndr from bronze.crm_cust_info

select DISTINCT cst_marital_status from bronze.crm_cust_info

--Table 2: bronze.crm_prd_info

select prd_id,count(*) as RecordCount from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null

--Check for unwanted Spaces
select prd_nm from 
bronze.crm_prd_info
where prd_nm <> TRIM(prd_nm)

--Check for null or Negative cost
select prd_cost from 
bronze.crm_prd_info
where prd_cost < 0 OR prd_cost is NULL

--Data Standardization and Consistency
select distinct prd_line
from bronze.crm_prd_info

--Check for invalid dates (Start date > End date)
select *
from bronze.crm_prd_info
where prd_start_dt > prd_end_dt

--Handling Invalid dates
SELECT 
prd_id, 
prd_key,
prd_nm,
prd_start_dt,
LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt) - 1 as testdate,
prd_end_dt
from bronze.crm_prd_info
where prd_key in (
'AC-HE-HL-U509-R',
'AC-HE-HL-U509')

--Table 3: bronze.crm_sales_details

--Ensuring there is no leading or trailing spaces
select sls_ord_num from 
bronze.crm_sales_details
where sls_ord_num <> TRIM(sls_ord_num)

select prd_key from silver.crm_prd_info
select sls_prd_key from bronze.crm_sales_details

--Check for invalid dates and Handling Them
select 
NULLIF(sls_order_dt, 0) AS sls_order_dt
from bronze.crm_sales_details
WHERE len(sls_order_dt) != 8
OR sls_order_dt < 19000101
OR sls_order_dt > 20250101
OR sls_order_dt <= 0

select 
NULLIF(sls_ship_dt, 0) AS sls_ship_dt
from bronze.crm_sales_details
WHERE len(sls_ship_dt) != 8
OR sls_ship_dt < 19000101
OR sls_ship_dt > 20250101
OR sls_ship_dt <= 0

select 
NULLIF(sls_due_dt, 0) AS sls_due_dt
from bronze.crm_sales_details
WHERE len(sls_due_dt) != 8
OR sls_due_dt < 19000101
OR sls_due_dt > 20250101
OR sls_due_dt <= 0


select 
* from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- 
select
sls_sales,
sls_quantity,
sls_price
from 
bronze.crm_sales_details
where sls_sales <> sls_quantity * sls_price
OR sls_price is null or  sls_quantity is null or sls_sales is null
OR sls_price <= 0 or  sls_quantity <= 0 or sls_sales <= 0

--Table 4: bronze.erp_cust_az12
--Check for invalid dates & Out of range dates
select
cid,
bdate,
gen
from
bronze.erp_cust_az12
where bdate < '1950-01-01' OR bdate > GETDATE()

--Data Standardization & Consistency
select distinct gen from bronze.erp_cust_az12

--Table 5: bronze.erp_loc_a101

select 
cid,
cnrty
from bronze.erp_loc_a101

select cst_key from silver.crm_cust_info

--Data Standardization & Consistency
select distinct cnrty from bronze.erp_loc_a101

--Table 6: bronze.px_cat_g1v2

select id from bronze.erp_px_cat_g1v2 where id NOT IN
(select distinct prd_key from silver.crm_prd_info)

--Check for unwanted spaces & Data standardization and consistency
select distinct cat from bronze.erp_px_cat_g1v2
where cat <> TRIM(cat) OR subcat <> TRIM(subcat) OR maintenance <> TRIM(maintenance)

select distinct subcat from bronze.erp_px_cat_g1v2
select distinct cat from bronze.erp_px_cat_g1v2
select distinct maintenance from bronze.erp_px_cat_g1v2

