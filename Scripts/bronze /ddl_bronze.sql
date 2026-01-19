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

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @start_time DATETIME,
        @end_time   DATETIME,
        @duration   INT;

    BEGIN TRY
        PRINT '=====================================================';
        PRINT 'STARTING BRONZE LAYER LOAD';
        PRINT '=====================================================';

        /* =====================================================
           LOADING CRM TABLES
        ===================================================== */
        PRINT 'LOADING CRM TABLES';
        PRINT '-----------------------------------------------------';

        /* CRM CUSTOMER INFO */
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'D:\RESUME - ITM\baher\PAPU MOM\DATA WAREHOUSE END TO END\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        SET @duration = DATEDIFF(SECOND, @start_time, @end_time);

        PRINT 'crm_cust_info loaded in ' + CAST(@duration AS NVARCHAR(10)) + ' seconds';
        SELECT COUNT(*) AS record_count FROM bronze.crm_cust_info;


        /* CRM PRODUCT INFO */
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'D:\RESUME - ITM\baher\PAPU MOM\DATA WAREHOUSE END TO END\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        SET @duration = DATEDIFF(SECOND, @start_time, @end_time);

        PRINT 'crm_prd_info loaded in ' + CAST(@duration AS NVARCHAR(10)) + ' seconds';
        SELECT COUNT(*) AS record_count FROM bronze.crm_prd_info;


        /* CRM SALES DETAILS */
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'D:\RESUME - ITM\baher\PAPU MOM\DATA WAREHOUSE END TO END\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        SET @duration = DATEDIFF(SECOND, @start_time, @end_time);

        PRINT 'crm_sales_details loaded in ' + CAST(@duration AS NVARCHAR(10)) + ' seconds';
        SELECT COUNT(*) AS record_count FROM bronze.crm_sales_details;


        /* =====================================================
           LOADING ERP TABLES
        ===================================================== */
        PRINT '-----------------------------------------------------';
        PRINT 'LOADING ERP TABLES';
        PRINT '-----------------------------------------------------';

        /* ERP CUSTOMER */
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\RESUME - ITM\baher\PAPU MOM\DATA WAREHOUSE END TO END\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        SET @duration = DATEDIFF(SECOND, @start_time, @end_time);

        PRINT 'erp_cust_az12 loaded in ' + CAST(@duration AS NVARCHAR(10)) + ' seconds';
        SELECT COUNT(*) AS record_count FROM bronze.erp_cust_az12;


        /* ERP LOCATION */
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\RESUME - ITM\baher\PAPU MOM\DATA WAREHOUSE END TO END\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        SET @duration = DATEDIFF(SECOND, @start_time, @end_time);

        PRINT 'erp_loc_a101 loaded in ' + CAST(@duration AS NVARCHAR(10)) + ' seconds';
        SELECT COUNT(*) AS record_count FROM bronze.erp_loc_a101;


        /* ERP PRODUCT CATEGORY */
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\RESUME - ITM\baher\PAPU MOM\DATA WAREHOUSE END TO END\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        SET @duration = DATEDIFF(SECOND, @start_time, @end_time);

        PRINT 'erp_px_cat_g1v2 loaded in ' + CAST(@duration AS NVARCHAR(10)) + ' seconds';
        SELECT COUNT(*) AS record_count FROM bronze.erp_px_cat_g1v2;


        PRINT '=====================================================';
        PRINT 'BRONZE LAYER LOAD COMPLETED SUCCESSFULLY';
        PRINT '=====================================================';

    END TRY
    BEGIN CATCH
        PRINT '=====================================================';
        PRINT 'ERROR OCCURRED IN BRONZE LOAD';
        PRINT ERROR_MESSAGE();
        PRINT '=====================================================';
    END CATCH
END;
GO
