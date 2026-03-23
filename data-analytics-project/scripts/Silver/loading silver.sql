
CREATE OR ALTER PROCEDURE silver.load_silver 
AS
DECLARE 
    @start_time DATETIME, 
    @end_time DATETIME
BEGIN

    SET NOCOUNT ON;

    BEGIN TRY

        PRINT 'Loading Silver Layer';

		-- Loading silver.crm_cust_info
        SET @start_time = GETDATE();

		TRUNCATE TABLE silver.crm_customer_info;
		INSERT INTO silver.crm_customer_info (
			customer_id, 
			customer_key, 
			customer_firstname, 
			customer_lastname, 
			customer_marital_status, 
			customer_gndr,
			customer_create_date
		)
		SELECT
			customer_id,
			customer_key,
			TRIM(customer_firstname) AS customer_firstname,
			TRIM(customer_lastname) AS customer_lastname,
			CASE 
				WHEN UPPER(TRIM(customer_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(customer_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS customer_marital_status, -- Normalize marital status
			CASE 
				WHEN UPPER(TRIM(customer_gender)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(customer_gender)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS customer_gender, -- Normalize gender values to readable format
			customer_create_date
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_create_date DESC) AS flag_last
			FROM bronze.crm_customer_info
			WHERE customer_id IS NOT NULL
		) t
		WHERE flag_last = 1; -- Select the most recent record per customer
		SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
        
		-- Loading silver.crm_prd_info
        SET @start_time = GETDATE();
		
		TRUNCATE TABLE silver.crm_product_info;
		INSERT INTO silver.crm_product_info (
			product_id,
			category_id,
			product_key,
			product_nm,
			product_cost,
			product_line,
			product_start_dt,
			product_end_dt
		)
		SELECT
			product_id,
			REPLACE(SUBSTRING(product_key, 1, 5), '-', '_') AS category_id, -- Extract category ID
			SUBSTRING(product_key, 7, LEN(product_key)) AS product_key,        -- Extract product key
			product_nm,
			ISNULL(product_cost, 0) AS product_cost,
			CASE 
				WHEN UPPER(TRIM(product_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(product_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(product_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(product_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS product_line, -- Give full description of product line
			CAST(product_start_dt AS DATE) AS product_start_dt,
			CAST(
				LEAD(product_start_dt) OVER (PARTITION BY product_key ORDER BY product_start_dt) - 1 
				AS DATE
			) AS product_end_dt -- Calculate end date as one day before the next start date
		FROM bronze.crm_product_info;
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
        
        -- Loading crm_sales_details
        SET @start_time = GETDATE();
		
		TRUNCATE TABLE silver.crm_sales_details;
		INSERT INTO silver.crm_sales_details (
			sales_ord_num,
			sales_prd_key,
			sales_cust_id,
			sales_order_dt,
			sales_ship_dt,
			sales_due_dt,
			sales_sales,
			sales_quantity,
			sales_price
		)
		SELECT 
			sales_ord_num,
			sales_prd_key,
			sales_cust_id,
			CASE 
				WHEN sales_order_dt = 0 OR LEN(sales_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sales_order_dt AS VARCHAR) AS DATE)
			END AS sales_order_dt,
			CASE 
				WHEN sales_ship_dt = 0 OR LEN(sales_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sales_ship_dt AS VARCHAR) AS DATE)
			END AS sales_ship_dt,
			CASE 
				WHEN sales_due_dt = 0 OR LEN(sales_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sales_due_dt AS VARCHAR) AS DATE)
			END AS sales_due_dt,
			CASE 
				WHEN sales_sales IS NULL OR sales_sales <= 0 OR sales_sales != sales_quantity * ABS(sales_price) 
					THEN sales_quantity * ABS(sales_price)
				ELSE sales_sales
			END AS sales_sales, -- Recalculate sales if original value is missing or incorrect
			sales_quantity,
			CASE 
				WHEN sales_price IS NULL OR sales_price <= 0 
					THEN sales_sales / NULLIF(sales_quantity, 0)
				ELSE sales_price  -- Derive price if original value is invalid
			END AS sales_price
		FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
        
        -- Loading erp_cust_az12
        SET @start_time = GETDATE();
		
		TRUNCATE TABLE silver.erp_cust_az12;
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
				ELSE cid
			END AS cid, 
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate, -- Set future birthdates to NULL
			CASE
				WHEN UPPER(TRIM(gender)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gender)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen -- Normalize gender values and handle unknown cases
		FROM bronze.erp_cust_az12;
	    SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
       
		PRINT 'Loading ERP Tables';
        SET @start_time = GETDATE();
		
		TRUNCATE TABLE silver.erp_loc_a101;
		INSERT INTO silver.erp_loc_a101 (
			cid,
			country
		)
		SELECT
			REPLACE(cid, '-', '') AS cid, 
			CASE
				WHEN TRIM(country) = 'DE' THEN 'Germany'
				WHEN TRIM(country) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(country) = '' OR country IS NULL THEN 'n/a'
				ELSE TRIM(country)
			END AS country -- Normalize and Handle missing or blank country codes
		FROM bronze.erp_loc_a101;
	    SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';
       
		
		-- Loading erp_px_cat_g1v2
		SET @start_time = GETDATE();
	
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
       
		PRINT 'Loading Silver Layer is Completed';
        	
	END TRY
	BEGIN CATCH
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS VARCHAR);
	END CATCH
END


EXEC silver.load_silver;

