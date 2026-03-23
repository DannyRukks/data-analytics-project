CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
DECLARE 
    @start_time DATETIME,
    @end_time DATETIME,
    @batch_start_time DATETIME,
    @batch_end_time DATETIME;
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY

        PRINT 'Loading Bronze Layer Started';

        SET @start_time = GETDATE();
        
        PRINT 'Loading: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_customer_info;

        BULK INSERT bronze.crm_customer_info
        FROM 'C:\Users\hp\Documents\Data Engineering Project\SQL Data Warehouse Project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',');

        SET @end_time = GETDATE();
        PRINT 'Duration: ' + 
              CAST(DATEDIFF(SECOND,@start_time, @end_time) AS VARCHAR) 
              + ' seconds';


        PRINT 'Loading: bronze.crm_product_info';
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_product_info;
        BULK INSERT bronze.crm_product_info
        FROM 'C:\Users\hp\Documents\Data Engineering Project\SQL Data Warehouse Project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',');

        SET @end_time = GETDATE();
        PRINT 'Duration: ' + 
              CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) 
              + ' seconds';


        PRINT 'Loading: bronze.crm_sales_details';
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_sales_details;
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\hp\Documents\Data Engineering Project\SQL Data Warehouse Project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',');

        SET @end_time = GETDATE();
        PRINT 'Duration: ' + 
              CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) 
              + ' seconds';


        PRINT 'Loading: bronze.erp_loc_a101';
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.erp_loc_a101;
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\hp\Documents\Data Engineering Project\SQL Data Warehouse Project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',');

        SET @end_time = GETDATE();
        PRINT 'Duration: ' + 
              CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) 
              + ' seconds';

        PRINT 'Loading: bronze.erp_cust_az12';
        SET @start_time = GETDATE();

        
        TRUNCATE TABLE bronze.erp_cust_az12;
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\hp\Documents\Data Engineering Project\SQL Data Warehouse Project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',');

        SET @end_time = GETDATE();
        PRINT 'Duration: ' + 
              CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) 
              + ' seconds';


        PRINT 'Loading: bronze.erp_px_cat_g1v2';
        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\hp\Documents\Data Engineering Project\SQL Data Warehouse Project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',');

        SET @end_time = GETDATE();
        PRINT 'Duration: ' + 
              CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR) 
              + ' seconds';

        PRINT 'Bronze Layer Load Completed';
        PRINT 'Total Duration: ' +
              CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS VARCHAR)
              + ' seconds';
    END TRY
    BEGIN CATCH
        PRINT 'ERROR DURING BRONZE LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
    END CATCH
END;

EXEC bronze.load_bronze;

SELECT TOP (5) *
FROM bronze.erp_px_cat_g1v2;