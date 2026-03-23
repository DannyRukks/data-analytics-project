
DROP VIEW IF EXISTS gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key,
    ci.customer_id              AS customer_id,
    ci.customer_key             AS customer_number,
    ci.customer_firstname       AS first_name,
    ci.customer_lastname        AS last_name,
    la.country                  AS country,
    ci.customer_marital_status  AS marital_status,
    CASE 
        WHEN ci.customer_gndr != 'n/a' THEN ci.customer_gndr 
        ELSE COALESCE(ca.gen, 'n/a')  			 
    END  AS gender,
    ca.bdate  AS birthdate,
    ci.customer_create_date AS create_date 
FROM silver.crm_customer_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.customer_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.customer_key = la.cid;
GO

DROP VIEW IF EXISTS gold.products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.product_start_dt, pn.product_key) AS product_key,
    pn.product_id       AS product_id,
    pn.product_key      AS product_number,
    pn.product_nm       AS product_name,
    pn.category_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.product_cost     AS cost,
    pn.product_line     AS product_line,
    pn.product_start_dt AS start_date
FROM silver.crm_product_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.category_id = pc.id
WHERE pn.product_end_dt IS NULL; -- Filter out all historical data
GO

DROP VIEW IF EXISTS gold.sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sales_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sales_order_dt AS order_date,
    sd.sales_ship_dt  AS shipping_date,
    sd.sales_due_dt   AS due_date,
    sd.sales_sales    AS sales_amount,
    sd.sales_quantity AS quantity,
    sd.sales_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sales_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sales_cust_id = cu.customer_id;
GO

