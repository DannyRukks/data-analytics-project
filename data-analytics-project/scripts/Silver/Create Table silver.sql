-- Create Tables in the Silver Schema

CREATE SCHEMA silver;
GO

DROP TABLE IF EXISTS silver.crm_customer_info
CREATE TABLE silver.crm_customer_info (
    customer_id             INT,
    customer_key            VARCHAR(50),
    customer_firstname      VARCHAR(50),
    customer_lastname       VARCHAR(50),
    customer_marital_status VARCHAR(50),
    customer_gndr           VARCHAR(50),
    customer_create_date    DATE,
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);

DROP TABLE IF EXISTS silver.crm_product_info
CREATE TABLE silver.crm_product_info (
    product_id          INT,
    category_id         VARCHAR(50),
    product_key         VARCHAR(50),
    product_nm          VARCHAR(50),
    product_cost        INT,
    product_line        VARCHAR(50),
    product_start_dt    DATE,
    product_end_dt      DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

DROP TABLE IF EXISTS silver.crm_sales_details
CREATE TABLE silver.crm_sales_details (
    sales_ord_num     NVARCHAR(50),
    sales_prd_key     NVARCHAR(50),
    sales_cust_id     INT,
    sales_order_dt    DATE,
    sales_ship_dt     DATE,
    sales_due_dt      DATE,
    sales_sales       INT,
    sales_quantity    INT,
    sales_price       INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

DROP TABLE IF EXISTS silver.erp_loc_a101b
CREATE TABLE silver.erp_loc_a101 (
    cid             VARCHAR(50),
    country         VARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
    cid             VARCHAR(50),
    bdate           DATE,
    gen             VARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
    id              VARCHAR(50),
    cat             VARCHAR(50),
    subcat          VARCHAR(50),
    maintenance     VARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

