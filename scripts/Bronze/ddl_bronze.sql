--Creating the database
CREATE DATABASE DataWarehouse;

use DataWarehouse;

--Creating the schemas(Bronze, Silver & Gold)

CREATE SCHEMA bronze;
Go

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
----------------- Main code Begins Here --------------------
  

IF OBJECT_ID('bronze.crm_cust_info', 'U') is not null
	drop table bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info(
	cst_id             INT,
	cst_key            nvarchar(50),
	cst_firstname      nvarchar(50),
	cst_lastname       nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr           nvarchar(50),
	cst_create_date    date
);
GO

IF OBJECT_ID('bronze.crm_prd_info', 'U') is not null
	drop table bronze.crm_prd_info;
GO

create table bronze.crm_prd_info(
	prd_id             int,
	prd_key            nvarchar(50),
	prd_nm             nvarchar(50),
	prd_cost           int,
	prd_line           nvarchar(50),
	prd_start_dt       datetime,
	prd_end_dt         datetime
);

IF OBJECT_ID('bronze.crm_sales_details', 'U') is not null
	drop table bronze.crm_sales_details
create table bronze.crm_sales_details(
	sls_ord_num   nvarchar(50),
	sls_prd_key   nvarchar(50),
	sls_cust_id   int,
	sls_order_dt  int,
	sls_ship_dt   int,
	sls_due_dt    int,
	sls_sales     int,
	sls_quantity  int,
	sls_price     int
);
GO

IF OBJECT_ID('bronze.erp_cust_az12', 'U') is not null
	drop table bronze.erp_cust_az12;
GO

create table bronze.erp_cust_az12(
	cid    nvarchar(50),
	bdate  date,
	gen    nvarchar(50)
);
GO

IF OBJECT_ID('bronze.erp_loc_a101', 'U') is not null
	drop table bronze.erp_loc_a101;
GO

create table bronze.erp_loc_a101(
	cid   nvarchar(50),
	cnrty nvarchar(50)
);
GO
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') is not null
	drop table bronze.erp_px_cat_g1v2;
GO

create table bronze.erp_px_cat_g1v2(
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50)
);
GO

