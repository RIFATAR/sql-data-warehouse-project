/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================

Script Purpose:
    This script defines the views for the Gold layer of the data warehouse, implementing the final star schema structure with clear separation of fact and dimension tables.
    - The Gold layer delivers cleaned, conformed, and business-ready datasets for advanced analytics and reporting.
    - Each view applies critical transformations, enrichment, and integration of data from the Silver layer, ensuring consistency, quality, and usability for decision-making.

Design Principles:
    - Dimensional Modeling: Gold views are modeled as star schema tables (dimensions and facts) to maximize query performance and simplify business analysis.
    - Data Enrichment & Cleansing: Data from multiple sources is harmonized—missing values are handled, additional attributes added, and business logic applied to ensure reliability.
    - Auditability: Transformations are traceable back to Silver layer sources, supporting data lineage and audit trails.

Usage:
    - These views serve as the primary interface for BI tools, dashboards, and business reports.
    - Analysts and end-users can query gold views directly to generate actionable insights and monitor key business metrics with minimal technical overhead.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
/*
    gold.dim_customers:
    - Creates the customer dimension view with a surrogate key ('customer_key')
    - Combines CRM (primary) and ERP sources to clean and enrich the customer profile
    - Ensures gender is primarily from CRM with fallback to ERP
    - LEFT JOINs ensure full customer coverage, even if details are missing in ERP

    Sequence Reason:
    - Must be defined before fact tables, as facts reference customer dimension via 'customer_key'
*/
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key for dim table
    ci.cst_id                          AS customer_id,   -- Natural key from CRM
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.country,                                         -- Country info from ERP location
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr       -- Prefer CRM gender value
        ELSE COALESCE(ca.gen, 'n/a')                     -- ERP fallback for gender
    END                                AS gender,
    ca.bdate                           AS birthdate,     -- Date of birth from ERP
    ci.cst_create_date                 AS create_date    -- CRM account creation date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
/*
    gold.dim_products:
    - Creates the product dimension view with surrogate key ('product_key')
    - Enriches product data by joining with category definitions for analytics
    - Filters OUT historical products (by ensuring 'prd_end_dt IS NULL')

    Sequence Reason:
    - Must be defined before fact tables, as facts reference product dimension via 'product_key'
*/
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,                         -- Natural key
    pn.prd_key      AS product_number,                     -- Alt. product identifier
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,                           -- Category enrichment
    pc.subcat       AS subcategory,                        -- Subcategory enrichment
    pc.maintenance  AS maintenance,                        -- Maintenance flag/info
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date                          -- Product launch date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Only active products
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
/*
    gold.fact_sales:
    - Creates sales fact view, joining the above dimension views
    - Ensures referential integrity via surrogate keys (customer_key, product_key)
    - Brings together order, shipping, due dates with monetary and quantity values

    Sequence Reason:
    - Must be created AFTER dimensions, since it depends on dimension surrogate keys.
    - Implements Star Schema: fact table central with foreign key links to dimensions.
*/
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,                  -- Primary sales order ID
    pr.product_key  AS product_key,                   -- FK to product dimension
    cu.customer_key AS customer_key,                  -- FK to customer dimension
    sd.sls_order_dt AS order_date,                    -- Order date
    sd.sls_ship_dt  AS shipping_date,                 -- Shipping date
    sd.sls_due_dt   AS due_date,                      -- Payment due date
    sd.sls_sales    AS sales_amount,                  -- Total sales amount €
    sd.sls_quantity AS quantity,                      -- Quantity ordered
    sd.sls_price    AS price                          -- Price per item
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
GO
