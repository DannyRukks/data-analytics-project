DROP SCHEMA IF EXISTS bronze;
GO

CREATE SCHEMA bronze;
GO

DROP TABLE IF EXISTS bronze.crm_customer_info;
CREATE TABLE bronze.crm_customer_info (
    customer_id              INT,
    customer_key             VARCHAR(50),
    customer_firstname       VARCHAR(50),
    customer_lastname        VARCHAR(50),
    customer_marital_status  VARCHAR(50),
    customer_gender          VARCHAR(50),
    customer_create_date     DATE
);

DROP TABLE IF EXISTS bronze.crm_product_info
CREATE TABLE bronze.crm_product_info (
    product_id       INT,
    product_key      VARCHAR(50),
    product_nm       VARCHAR(50),
    product_cost     INT,
    product_line     VARCHAR(50),
    product_start_dt DATETIME,
    product_end_dt   DATETIME
);

DROP TABLE IF EXISTS bronze.crm_sales_details
CREATE TABLE bronze.crm_sales_details (
    sales_ord_num  VARCHAR(50),
    sales_prd_key  VARCHAR(50),
    sales_cust_id  INT,
    sales_order_dt INT,
    sales_ship_dt  INT,
    sales_due_dt   INT,
    sales_sales    INT,
    sales_quantity INT,
    sales_price    INT
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid    VARCHAR(50),
    country  VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gender VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50)
);
