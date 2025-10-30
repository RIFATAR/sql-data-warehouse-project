/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after loading the Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- 1. Checking 'silver.crm_cust_info' (Customer Master Table)
-- ====================================================================

-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results (Every customer should have a unique, non-null ID.)
SELECT
    cst_id,
    COUNT(*) AS occurrences
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for Unwanted Spaces in customer keys
-- Expectation: No Results (Keys should not have leading or trailing spaces.)
SELECT
    cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Data Standardization & Consistency: Marital Status
-- Use to verify allowed status values after ETL.
SELECT DISTINCT
    cst_marital_status
FROM silver.crm_cust_info;

-- ====================================================================
-- 2. Checking 'silver.crm_prd_info' (Product Master Table)
-- ====================================================================

-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results (Every product should have a unique, non-null ID.)
SELECT
    prd_id,
    COUNT(*) AS occurrences
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for Unwanted Spaces in product names
-- Expectation: No Results
SELECT
    prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLs or Negative Values in Cost
-- Expectation: No Results (Product costs should be positive and not null.)
SELECT
    prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistency: Product Line
-- Use to verify expected standard values ('Mountain', 'Road', etc.)
SELECT DISTINCT
    prd_line
FROM silver.crm_prd_info;

-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results (End date should not precede start date.)
SELECT
    *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ====================================================================
-- 3. Checking 'silver.crm_sales_details' (Sales Fact Table)
-- ====================================================================

-- Check for Invalid Dates in raw data (bronze layer) before transformation
-- Expectation: No Invalid Dates (Dates should be valid, within expected range.)
SELECT
    NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
    OR LEN(sls_due_dt) != 8
    OR sls_due_dt > 20500101
    OR sls_due_dt < 19000101;

-- Check for Invalid Date Orders: Order Date after Shipping/Due Date
-- Expectation: No Results (Orders should precede shipping and due dates.)
SELECT
    *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;

-- Check Data Consistency: Sales Value Calculations
-- Expectation: No Results (Sales should equal quantity times price.)
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- 4. Checking 'silver.erp_cust_az12' (ERP Customer Reference Table)
-- ====================================================================

-- Identify Out-of-Range Dates (Birthdates)
-- Expectation: Birthdates should be reasonable range (not in future, not before 1924)
SELECT DISTINCT
    bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01'
   OR bdate > GETDATE();

-- Data Standardization & Consistency: Gender
SELECT DISTINCT
    gen
FROM silver.erp_cust_az12;

-- ====================================================================
-- 5. Checking 'silver.erp_loc_a101' (ERP Location Table)
-- ====================================================================

-- Data Standardization & Consistency: Country Codes/Names
-- Test: List all distinct country values for manual review.
-- Use to check for spelling errors, unexpected values, etc.
SELECT DISTINCT
    country
FROM silver.erp_loc_a101
ORDER BY country;

-- ====================================================================
-- 6. Checking 'silver.erp_px_cat_g1v2' (Product Category Table)
-- ====================================================================

-- Check for Unwanted Spaces in category/subcategory/maintenance fields
-- Expectation: No Results (No leading/trailing spaces in these columns.)
SELECT
    *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency: Maintenance Status
SELECT DISTINCT
    maintenance
FROM silver.erp_px_cat_g1v2;

-- ====================================================================
-- 7. Checking for Orphan/Unmatched Foreign Keys
-- ====================================================================
-- Test: Find sales records referencing non-existent customers.
-- Expectation: No Results (All sales should point to valid customers.)
SELECT
    s.sls_ord_num,
    s.sls_cust_id
FROM silver.crm_sales_details s
LEFT JOIN silver.crm_cust_info c
    ON s.sls_cust_id = c.cst_id
WHERE c.cst_id IS NULL;

-- Test: Find sales records referencing non-existent products.
-- Expectation: No Results (All sales should point to valid products.)
SELECT
    s.sls_ord_num,
    s.sls_prd_key
FROM silver.crm_sales_details s
LEFT JOIN silver.crm_prd_info p
    ON s.sls_prd_key = p.prd_key
WHERE p.prd_key IS NULL;

-- ====================================================================
-- 8. Unused/Inactive Product or Customer Entries
-- ====================================================================
-- Test: Find customers in master who have never placed an order.
-- Expectation: Some results possible; use for cleanup and analysis.
SELECT
    c.cst_id
FROM silver.crm_cust_info c
LEFT JOIN silver.crm_sales_details s
    ON c.cst_id = s.sls_cust_id
WHERE s.sls_cust_id IS NULL;

-- Test: Find products never sold.
-- Expectation: Some results possible; use for inventory management.
SELECT
    p.prd_key
FROM silver.crm_prd_info p
LEFT JOIN silver.crm_sales_details s
    ON p.prd_key = s.sls_prd_key
WHERE s.sls_prd_key IS NULL;

-- ====================================================================
-- 9. Numeric Fields Validity Checks
-- ====================================================================
-- Test: Find negative sales quantities (should not be possible).
-- Expectation: No Results.
SELECT
    sls_ord_num,
    sls_quantity
FROM silver.crm_sales_details
WHERE sls_quantity < 0;

-- Test: Find negative or zero sales price (should not be possible).
-- Expectation: No Results.
SELECT
    sls_ord_num,
    sls_price
FROM silver.crm_sales_details
WHERE sls_price <= 0;

-- ====================================================================
-- 10. Consistency in Reference Data
-- ====================================================================
-- Test: List all distinct country values for manual review.
-- Use to check for spelling errors, unexpected values, etc.
SELECT DISTINCT
    country
FROM silver.erp_loc_a101
ORDER BY country;
