/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '============================================================'
		PRINT 'Loading Silver Layer'
		PRINT '============================================================'

		PRINT '-------------------------------------------------------------'
		PRINT 'Loading CRM Tables'
		PRINT '-------------------------------------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info'; 
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT 'Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)
		SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 ELSE 'Unknown'
			 END as cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 ELSE 'Unknown'
			 END as cst_gndr,
		cst_create_date
		from (
		SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as last_flag
		FROM bronze.crm_cust_info
		WHERE cst_id is not null) t 
		WHERE last_flag = 1;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds'
		PRINT '>>---------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT 'Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
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
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id, --Extracted category ID
		SUBSTRING(prd_key, 7, len(prd_key)) as prd_key, --Extracted Product key
		prd_nm,
		ISNULL(prd_cost, 0) as prd_cost, --Replaced null with 0s
		CASE UPPER(TRIM(prd_line)) 
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'Unknown'
			 END AS prd_line, --Mapped product lines code to descriptive values
		CAST (prd_start_dt as date) as prd_start_dt,
		CAST(LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt) - 1 as date) as prd_end_dt --calculated end date as one day before the next start date
		from bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds'
		PRINT '>>---------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT 'Inserting Data Into: silver.crm_sales_details';

		INSERT INTO silver.crm_sales_details(
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
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) <> 8 THEN Null
			 ELSE CAST(CAST(sls_order_dt AS varchar) as DATE)
			 END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN Null
			 ELSE CAST(CAST(sls_ship_dt AS varchar) as DATE)
			 END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) <> 8 THEN Null
			 ELSE CAST(CAST(sls_due_dt AS varchar) as DATE)
			 END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> sls_quantity * ABS(sls_price)
			 THEN sls_quantity * ABS(sls_price)
			 ELSE sls_sales
			 END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0
			 THEN sls_sales / NULLIF(sls_quantity, 0)
			 ELSE sls_price
			 END AS sls_price
		from
		bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds'
		PRINT '>>---------------------'

		PRINT '-------------------------------------------------------'
		PRINT 'Loading ERP Tables'
		PRINT '-------------------------------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT 'Inserting Data Into: silver.erp_cust_az12';

		INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen
		)
		SELECT
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			 ELSE cid
			 END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			 ELSE bdate
			 END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			 WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			 ELSE 'Unknown'
			 END AS gen
		from bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds'
		PRINT '>>---------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT 'Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(cid, cnrty)
		SELECT 
		REPLACE(cid, '-', '') as cid,
		CASE WHEN TRIM(cnrty) ='DE' THEN 'Germany'
			 WHEN TRIM(cnrty) IN ('US', 'USA') THEN 'United States'
			 WHEN TRIM(cnrty) = '' THEN 'Unknown'
			 ELSE cnrty
			 END AS cnrty --Normalize and handle missing or blank country
		from bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds'
		PRINT '>>---------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT 'Inserting Data Into: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
		SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds'
		PRINT '>>---------------------'

		SET @batch_end_time = GETDATE();
		PRINT '======================================='
		PRINT 'Loading Silver Layer is Completed'
		PRINT '    - Total Duration is :' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + 'Seconds'
		PRINT '======================================='
	END TRY
	BEGIN CATCH
		PRINT '========================================'
		PRINT 'Error Occured During Loading Silver Layer'
		PRINT 'Error Message :' + ERROR_MESSAGE();
		PRINT 'Error Message :' + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error Message :' + CAST(ERROR_STATE() AS VARCHAR);
		PRINT '========================================'
	END CATCH
END
