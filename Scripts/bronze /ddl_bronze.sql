/*
============================================================
Procedure Name : bronze.load_bronze
Layer          : Bronze (Raw Ingestion Layer)
Purpose        : Load raw data from CRM and ERP source files
                 into Bronze tables using BULK INSERT.

Description:
This procedure performs a full refresh of all Bronze tables by:
- Truncating existing Bronze tables
- Loading raw data from CSV source files
- Measuring load duration for each table
- Measuring total execution time for the entire Bronze load
- Logging progress in real time using RAISERROR WITH NOWAIT

Key Design Decisions:
- Uses TRUNCATE + BULK INSERT for fast, full loads
- No transformations are applied (raw data ingestion only)
- Table-level and pipeline-level timing is captured
- Real-time logging is enabled to monitor long-running loads
- TRY/CATCH is used for basic error handling

Why This Matters:
The Bronze layer is responsible for safe, scalable, and traceable
raw data ingestion. Accurate logging and timing at this stage
ensure transparency, easier debugging, and reliable downstream
processing in Silver and Gold layers.

Execution:
EXEC bronze.load_bronze;

============================================================
*/
create table bronze.crm_cust_info (
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_material_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date Date
);


create table bronze.crm_prd_info(
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int ,
prd_line nvarchar(50),
prd_start_dt Datetime,
prd_end_dt Datetime
);

Create Table bronze.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);

Create Table bronze.erp_loc_a101(
cid nvarchar(50),
cntry nvarchar(50),
);

create table bronze.erp_cust_az12(
cid nvarchar(50),
bdate Date,
gen nvarchar(50),
);

create table bronze.erp_px_cat_g1v2(
id nvarchar(50),
cat nvarchar(50),
subcat  nvarchar(50),
maintance  nvarchar(50),
);
