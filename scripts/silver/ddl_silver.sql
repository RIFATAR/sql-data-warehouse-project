/*
===============================================================================
DDL Script: Create Silver Tables 
===============================================================================
Script Purpose:
    This script defines "silver" tables for the data warehouse ETL workflow.
    Drops existing tables if already defined.
    Use for direct loads from the "bronze" layer via ETL (see companion stored procedure).
    Data types and column names ensure compatibility with transformation logic.
===============================================================================
*/

/*------------------------------------------------------------------------------
  Customer Info Table
  ------------------------------------------------------------------------------
  Stores cleaned customer master data for analytics.
------------------------------------------------------------------------------*/
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
    cst_id             INT,             -- Surrogate customer ID from source
    cst_key            NVARCHAR(50),    -- Natural key/customer code
    cst_firstname      NVARCHAR(50),    -- First name (cleaned)
    cst_lastname       NVARCHAR(50),    -- Last name (cleaned)
    cst_marital_status NVARCHAR(50),    -- Marital status (normalized in ETL)
    cst_gndr           NVARCHAR(50),    -- Gender (normalized in ETL)
    cst_create_date    DATE,            -- Customer record create date
    dwh_create_date    DATETIME2 DEFAULT GETDATE() -- DW row creation timestamp
);
GO

/*------------------------------------------------------------------------------
  Product Info Table
  ------------------------------------------------------------------------------
  Stores cleansed and transformed product master data.
  Includes category ID derived from product key, date fields for lifecycle.
------------------------------------------------------------------------------*/
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id          INT,             -- Surrogate product ID
    cat_id          NVARCHAR(50),    -- Category ID (derived via ETL)
    prd_key         NVARCHAR(50),    -- Product key (possibly trimmed in ETL)
    prd_nm          NVARCHAR(50),    -- Product name (cleaned)
    prd_cost        INT,             -- Product cost (nulls handled in ETL)
    prd_line        NVARCHAR(50),    -- Product line (normalized names)
    prd_start_dt    DATE,            -- Product valid start date
    prd_end_dt      DATE,            -- Product valid end date (lagged)
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- DW row creation timestamp
);
GO

/*------------------------------------------------------------------------------
  Sales Details Table
  ------------------------------------------------------------------------------
  Stores sales order facts for reporting/aggregation.
  Dates and numeric values cleaned/derived by ETL logic.
------------------------------------------------------------------------------*/
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num     NVARCHAR(50),    -- Sales order number
    sls_prd_key     NVARCHAR(50),    -- Product key for sale
    sls_cust_id     INT,             -- Customer ID for sale
    sls_order_dt    DATE,            -- Sales order date (converted from int)
    sls_ship_dt     DATE,            -- Ship date
    sls_due_dt      DATE,            -- Order due date
    sls_sales       INT,             -- Sales amount (recalculated if necessary)
    sls_quantity    INT,             -- Quantity sold
    sls_price       INT,             -- Price (derived if missing/incorrect)
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- DW row creation timestamp
);
GO

/*------------------------------------------------------------------------------
  Location Table
  ------------------------------------------------------------------------------
  Stores customer location/country mapping.
  "country" column matches ETL and allows for consistent analytic joins.
------------------------------------------------------------------------------*/
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    cid             NVARCHAR(50),    -- Customer/location identifier
    country         NVARCHAR(50),    -- Country (normalized, e.g., 'Germany')
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- DW row creation timestamp
);
GO

/*------------------------------------------------------------------------------
  Customer Additional Properties Table
  ------------------------------------------------------------------------------
  Stores secondary customer properties, e.g., gender/birthdate from other sources.
------------------------------------------------------------------------------*/
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    cid             NVARCHAR(50),    -- Customer identifier
    bdate           DATE,            -- Birthdate (validated in ETL)
    gen             NVARCHAR(50),    -- Gender (normalized)
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- DW row creation timestamp
);
GO

/*------------------------------------------------------------------------------
  Product Category Table
  ------------------------------------------------------------------------------
  Stores product category definitions for enrichment and lookup.
------------------------------------------------------------------------------*/
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    id              NVARCHAR(50),    -- Category ID
    cat             NVARCHAR(50),    -- Category name
    subcat          NVARCHAR(50),    -- Subcategory name
    maintenance     NVARCHAR(50),    -- Maintenance info
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- DW row creation timestamp
);
GO
