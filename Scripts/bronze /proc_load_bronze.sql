/*
====================================================================================
Stored Procedure : bronze.load_bronze
Layer            : Bronze (Source -> Raw Ingestion)
====================================================================================

Script Purpose:
    This stored procedure is responsible for loading raw data into the
    'bronze' schema from external CSV source files. It represents the
    first step in the data warehouse ingestion pipeline.

Processing Details:
    - Performs a full refresh of Bronze tables using TRUNCATE + BULK INSERT
    - Loads data exactly as received from source systems (no transformations)
    - Captures load duration for each table
    - Captures total execution time for the complete Bronze load
    - Outputs real-time execution logs using RAISERROR WITH NOWAIT

Design Principles:
    - Bronze layer stores raw, immutable source data
    - Focus on performance, traceability, and reliability
    - Downstream cleansing and transformations are handled in Silver layer
    - Ensures minimal impact on source systems

Error Handling:
    - Uses TRY / CATCH to capture and log runtime errors
    - Fails fast to prevent partial or inconsistent loads

Parameters:
    None.
    This procedure does not accept input parameters and does not return values.

Usage Example:
    EXEC bronze.load_bronze;

====================================================================================
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
