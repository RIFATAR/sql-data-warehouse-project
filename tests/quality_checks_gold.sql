/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Block 1: Uniqueness Check - Customer Dimension
-- ====================================================================
/*
    Step 1: Validate uniqueness of surrogate keys (customer_key) in gold.dim_customers.
    Justification:
        - Dimension keys must be unique to reliably serve as primary/foreign keys in the star schema.
        - This check prevents duplication errors that would cascade to the fact table.

    Sequence:
        - Performed first, before product and fact checks, to ensure foundational dimension integrity.
*/
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;
-- Expectation: No results (no customer_key should be duplicated)

-- ====================================================================
-- Block 2: Uniqueness Check - Product Dimension
-- ====================================================================
/*
    Step 2: Validate uniqueness of surrogate keys (product_key) in gold.dim_products.
    Justification:
        - Ensures each product in the dimension table is unique.
        - Prevents join ambiguity and maintains correctness of fact table references.

    Sequence:
        - Checked after customer_key uniqueness to follow the order of dimension definition/use.
*/
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;
-- Expectation: No results (no product_key should be duplicated)

-- ====================================================================
-- Block 3: Referential Integrity - Fact and Dimension Connectivity
-- ====================================================================
/*
    Step 3: Test referential integrity between fact table and dimension tables.
    Justification:
        - Ensures every dimension reference in the fact table resolves to a valid row in the dimension tables.
        - Highlights missing dimension references (should be investigated as they break star schema integrity).

    Sequence:
        - Performed after uniqueness checks, since only unique keys can ensure trustworthy referential integrity.
        - Final step to validate the join relationships and full model connectivity for analytics.

    How to Interpret:
        - Any rows returned indicate a missing or orphaned key—these must be resolved before analytics.
*/
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL;
-- Expectation: No results (all reference keys in fact_sales should match the dimension tables)



-- ====================================================================
-- Block 4: Null Check - Surrogate Keys in Dimension Tables
-- ====================================================================
/*
    Step 4: Ensure no surrogate keys are NULL in dimension tables (customer_key, product_key).
    Justification:
        - NULL keys break referential integrity and cannot be reliably joined.
        - Critical for both analytics and downstream systems relying on dimension tables.
*/
SELECT * FROM gold.dim_customers WHERE customer_key IS NULL;
SELECT * FROM gold.dim_products WHERE product_key IS NULL;

-- ====================================================================
-- Block 5: Null/Invalid Key Check - Fact Table Foreign Keys
-- ====================================================================
/*
    Step 5: Ensure fact table does not contain NULLs or invalid foreign keys.
    Justification:
        - Prevents broken relationships in the star schema.
        - Protects aggregations and reporting accuracy.
*/
SELECT * FROM gold.fact_sales WHERE customer_key IS NULL OR product_key IS NULL;

-- ====================================================================
-- Block 6: Value Range & Completeness Checks
-- ====================================================================
/*
    Step 6: Business rule validation checks. Examples below:
    - Ensure sales_amount >= 0
    - Ensure quantity > 0
    - Validate birthdate (no future dates, reasonable range)
*/
SELECT * FROM gold.fact_sales WHERE sales_amount < 0;
SELECT * FROM gold.fact_sales WHERE quantity <= 0;
SELECT * FROM gold.dim_customers WHERE birthdate > GETDATE() OR birthdate < '1900-01-01';

-- ====================================================================
-- Block 7: Orphan Check - Customers/Products Not Referenced in Fact Table
-- ====================================================================
/*
    Step 7: Identify dimension records never referenced by the fact table.
    Justification:
        - Can help spot unlinked, inactive, or erroneous dimension records.
        - Useful for completeness and business review.
*/
SELECT c.* 
FROM gold.dim_customers c
LEFT JOIN gold.fact_sales f
    ON f.customer_key = c.customer_key
WHERE f.customer_key IS NULL;

SELECT p.* 
FROM gold.dim_products p
LEFT JOIN gold.fact_sales f
    ON f.product_key = p.product_key
WHERE f.product_key IS NULL;

-- ====================================================================
-- Block 8: Duplicate Natural Keys Check
-- ====================================================================
/*
    Step 8: Ensure natural business keys (e.g. customer_id, product_id) are unique.
    Justification:
        - Duplicate natural keys can signal ETL or data source errors.
        - Business integrity for analytics, compliance, and reporting.
*/
SELECT customer_id, COUNT(*) 
FROM gold.dim_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

SELECT product_id, COUNT(*)
FROM gold.dim_products
GROUP BY product_id
HAVING COUNT(*) > 1;

