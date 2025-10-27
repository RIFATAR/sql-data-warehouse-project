-- =============================================================
-- STEP 1: CREATE DATABASE AND INITIAL SCHEMAS
-- =============================================================

USE master;

-- Create a dedicated Data Warehouse database
CREATE DATABASE DataWareHouse;
GO

-- Switch context to the newly created database
USE DataWareHouse;
GO

-- Create schemas representing different data processing layers
CREATE SCHEMA bronze;   -- Raw or lightly processed source data (initial ingestion layer)
CREATE SCHEMA silver;   -- Cleaned and transformed data (business-ready but still detailed)
GO
CREATE SCHEMA golden;   -- Curated, aggregated, and analytical data (for BI/Reports)
GO



-- =============================================================
-- STEP 2: CREATE BRONZE LAYER TABLES (RAW INGESTION LAYER)
-- =============================================================


-- ------------------------------
-- CRM Customer Info Table
-- ------------------------------
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,                     -- Customer ID (unique identifier)
    cst_key NVARCHAR(50),           -- Source system customer key
    cst_firstname NVARCHAR(50),     -- Customer first name
    cst_lastname NVARCHAR(50),      -- Customer last name
    cst_marital_status NVARCHAR(50),-- Marital status (e.g., Single, Married)
    cst_gndr NVARCHAR(50),          -- Gender
    cst_create_date DATE            -- Record creation date in CRM
);



-- ------------------------------
-- CRM Product Info Table
-- ------------------------------
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,                     -- Product ID (unique identifier)
    prd_key NVARCHAR(50),           -- Source system product key
    prd_nm NVARCHAR(50),            -- Product name
    prd_cost INT,                   -- Product cost value
    prd_line NVARCHAR(50),          -- Product line/category (e.g., Electronics)
    prd_start_dt DATETIME,          -- Product availability start date
    prd_end_dt DATETIME             -- Product availability end date
);



-- ------------------------------
-- CRM Sales Details Table
-- ------------------------------
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num NVARCHAR(50),       -- Sales order number
    sls_prd_key NVARCHAR(50),       -- Product key (foreign reference to product table)
    sls_cust_id INT,                -- Customer ID (foreign reference to customer table)
    sls_order_dt INT,               -- Order date (stored as integer YYYYMMDD)
    sls_ship_dt INT,                -- Shipping date
    sls_due_dt INT,                 -- Due date
    sls_sales INT,                  -- Total sales amount
    sls_quantity INT,               -- Number of items sold
    sls_price INT                   -- Price per unit
);



-- ------------------------------
-- ERP Customer Data Table
-- ------------------------------
CREATE TABLE bronze.erp_cust_az12 (
    cid NVARCHAR(50),               -- Customer unique ID in ERP system
    bdate DATE,                     -- Birthdate of the customer
    gen NVARCHAR(50)                -- Gender (may differ from CRM format)
);



-- ------------------------------
-- ERP Product Category Table
-- ------------------------------
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id NVARCHAR(50),                -- Product category unique ID
    cat NVARCHAR(50),               -- Main category
    subcat NVARCHAR(50),            -- Subcategory (detailed level)
    maintenance NVARCHAR(50)        -- Maintenance or product condition info
);



-- ------------------------------
-- ERP Location Data Table
-- ------------------------------
CREATE TABLE bronze.erp_loc_a101 (
    cid NVARCHAR(50),               -- Customer or site ID
    country NVARCHAR(50)            -- Country of operation or residence
);
GO
