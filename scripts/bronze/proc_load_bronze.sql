-- ===============================================================
-- Author:      [Rifat]
-- Created On:  2025-10-26
-- Module:      Bronze Layer Load Procedure
-- Description: Loads all Bronze-layer CSVs with timing per table
--               and calculates total batch duration.
-- ===============================================================



/*
What this procedure does (bronze.load_bronze):
- Starts and logs the batch ETL process for the Bronze layer.
- Records the start time for tracking overall duration.
- Executes ETL operations in a transaction for safe load/rollback.
- For each target table (customer, product, sales):
    - Logs the table load start time.
    - Empties the table to avoid duplicates.
    - Loads latest data from the related CSV file.
    - Counts rows, logs load time, and records duration.
- Reports timing, row count, and status for each table and the full batch.
- Ensures data consistency and enables quick monitoring of load performance.


EXEC bronze.load_bronze;
-- This runs the procedure to load CRM customer, product, and sales data
-- from CSVs into the Bronze layer tables, showing progress, row counts, and timing.

*/



CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @batch_start_time DATETIME,
        @batch_end_time   DATETIME,
        @table_start      DATETIME,
        @table_end        DATETIME,
        @table_duration   INT,
        @batch_duration   INT,
        @table_name       NVARCHAR(100),
        @row_count        INT;

    BEGIN TRY
        PRINT '============================================================';
        PRINT 'Starting Bronze Layer Data Load (Batch Execution)...';
        PRINT '============================================================';

        SET @batch_start_time = GETDATE();  -- Mark batch start time

        BEGIN TRANSACTION;


        -- ===============================================================
        -- Step 1: Load CRM Customer Info
        -- ===============================================================
        SET @table_name = 'bronze.crm_cust_info';
        SET @table_start = GETDATE();
        PRINT CHAR(10) + '--- Loading Table: ' + @table_name + 
              ' | Start: ' + CONVERT(VARCHAR(30), @table_start, 120);

        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SELECT @row_count = COUNT(*) FROM bronze.crm_cust_info;
        SET @table_end = GETDATE();
        SET @table_duration = DATEDIFF(SECOND, @table_start, @table_end);

        PRINT 'Rows Loaded: ' + CAST(@row_count AS NVARCHAR(10));
        PRINT 'End Time:    ' + CONVERT(VARCHAR(30), @table_end, 120);
        PRINT 'Duration:    ' + CAST(@table_duration AS NVARCHAR(10)) + ' seconds';


        -- ===============================================================
        -- Step 2: Load CRM Product Info
        -- ===============================================================
        SET @table_name = 'bronze.crm_prd_info';
        SET @table_start = GETDATE();
        PRINT CHAR(10) + '--- Loading Table: ' + @table_name + 
              ' | Start: ' + CONVERT(VARCHAR(30), @table_start, 120);

        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SELECT @row_count = COUNT(*) FROM bronze.crm_prd_info;
        SET @table_end = GETDATE();
        SET @table_duration = DATEDIFF(SECOND, @table_start, @table_end);

        PRINT 'Rows Loaded: ' + CAST(@row_count AS NVARCHAR(10));
        PRINT 'End Time:    ' + CONVERT(VARCHAR(30), @table_end, 120);
        PRINT 'Duration:    ' + CAST(@table_duration AS NVARCHAR(10)) + ' seconds';



        -- ===============================================================
        -- Step 3: Load CRM Sales Details
        -- ===============================================================
        SET @table_name = 'bronze.crm_sales_details';
        SET @table_start = GETDATE();
        PRINT CHAR(10) + '--- Loading Table: ' + @table_name + 
              ' | Start: ' + CONVERT(VARCHAR(30), @table_start, 120);

        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SELECT @row_count = COUNT(*) FROM bronze.crm_sales_details;
        SET @table_end = GETDATE();
        SET @table_duration = DATEDIFF(SECOND, @table_start, @table_end);

        PRINT 'Rows Loaded: ' + CAST(@row_count AS NVARCHAR(10));
        PRINT 'End Time:    ' + CONVERT(VARCHAR(30), @table_end, 120);
        PRINT 'Duration:    ' + CAST(@table_duration AS NVARCHAR(10)) + ' seconds';



        -- ===============================================================
        -- Step 4: Load ERP Customer Data
        -- ===============================================================
        SET @table_name = 'bronze.erp_cust_az12';
        SET @table_start = GETDATE();
        PRINT CHAR(10) + '--- Loading Table: ' + @table_name + 
              ' | Start: ' + CONVERT(VARCHAR(30), @table_start, 120);

        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SELECT @row_count = COUNT(*) FROM bronze.erp_cust_az12;
        SET @table_end = GETDATE();
        SET @table_duration = DATEDIFF(SECOND, @table_start, @table_end);

        PRINT 'Rows Loaded: ' + CAST(@row_count AS NVARCHAR(10));
        PRINT 'End Time:    ' + CONVERT(VARCHAR(30), @table_end, 120);
        PRINT 'Duration:    ' + CAST(@table_duration AS NVARCHAR(10)) + ' seconds';



        -- ===============================================================
        -- Step 5: Load ERP Product Category
        -- ===============================================================
        SET @table_name = 'bronze.erp_px_cat_g1v2';
        SET @table_start = GETDATE();
        PRINT CHAR(10) + '--- Loading Table: ' + @table_name + 
              ' | Start: ' + CONVERT(VARCHAR(30), @table_start, 120);

        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SELECT @row_count = COUNT(*) FROM bronze.erp_px_cat_g1v2;
        SET @table_end = GETDATE();
        SET @table_duration = DATEDIFF(SECOND, @table_start, @table_end);

        PRINT 'Rows Loaded: ' + CAST(@row_count AS NVARCHAR(10));
        PRINT 'End Time:    ' + CONVERT(VARCHAR(30), @table_end, 120);
        PRINT 'Duration:    ' + CAST(@table_duration AS NVARCHAR(10)) + ' seconds';



        -- ===============================================================
        -- Step 6: Load ERP Location Data
        -- ===============================================================
        SET @table_name = 'bronze.erp_loc_a101';
        SET @table_start = GETDATE();
        PRINT CHAR(10) + '--- Loading Table: ' + @table_name + 
              ' | Start: ' + CONVERT(VARCHAR(30), @table_start, 120);

        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SELECT @row_count = COUNT(*) FROM bronze.erp_loc_a101;
        SET @table_end = GETDATE();
        SET @table_duration = DATEDIFF(SECOND, @table_start, @table_end);

        PRINT 'Rows Loaded: ' + CAST(@row_count AS NVARCHAR(10));
        PRINT 'End Time:    ' + CONVERT(VARCHAR(30), @table_end, 120);
        PRINT 'Duration:    ' + CAST(@table_duration AS NVARCHAR(10)) + ' seconds';



        -- ===============================================================
        -- Batch Completion Summary
        -- ===============================================================
        COMMIT TRANSACTION;

        SET @batch_end_time = GETDATE();
        SET @batch_duration = DATEDIFF(SECOND, @batch_start_time, @batch_end_time);

        PRINT CHAR(10) + '============================================================';
        PRINT 'Bronze Layer Load Completed Successfully!';
        PRINT 'Batch Start : ' + CONVERT(VARCHAR(30), @batch_start_time, 120);
        PRINT 'Batch End   : ' + CONVERT(VARCHAR(30), @batch_end_time, 120);
        PRINT 'Total Duration: ' + CAST(@batch_duration AS NVARCHAR(10)) + ' seconds';
        PRINT '============================================================';
    END TRY


    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT '============================================================';
        PRINT 'ERROR: Bronze Layer Load Failed!';
        PRINT 'Table Name: ' + ISNULL(@table_name, 'UNKNOWN');
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT '============================================================';
    END CATCH;
END;
GO
