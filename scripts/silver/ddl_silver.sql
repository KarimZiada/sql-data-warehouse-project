/*
============================================================================
Quality Checks
============================================================================
Script Purpose:
  This script performs various quality checks for data consistency, accuracy,
  and standardization across the 'silver' schemas. 
  It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
  - Run these checks after data loading Silver Layer.
  - Investigate and resolve any discrepancies found during the checks.
  */


/* ================================================================================================
   SILVER TABLE: silver.crm_cust_info
   SOURCE TABLE: bronze.crm_cust_info
   ================================================================================================ */

--------------------------------------------------------------------------------------------------
-- 1.1 Detect customers with duplicate cst_id (business key duplicates)
--     (Silver keeps only the latest record per cst_id.)
--------------------------------------------------------------------------------------------------
SELECT 
    cst_id,
    COUNT(*) AS cnt_rows
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1
ORDER BY cnt_rows DESC, cst_id;

--------------------------------------------------------------------------------------------------
-- 1.2 Inspect all duplicate rows per customer, ordered by cst_create_date (latest first)
--     This supports the decision to keep only the latest record per cst_id in SILVER.
--------------------------------------------------------------------------------------------------
SELECT *
FROM (
    SELECT  *,
            ROW_NUMBER() OVER (
                PARTITION BY cst_id
                ORDER BY cst_create_date DESC
            ) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last > 1
ORDER BY cst_id, cst_create_date DESC;

--------------------------------------------------------------------------------------------------
-- 1.3 Check distribution of marital status values
--     Justifies mapping:
--       'S' -> 'Single', 'M' -> 'Married', ELSE 'n/a'
--------------------------------------------------------------------------------------------------
SELECT 
    TRIM(cst_marital_status) AS raw_marital_status,
    COUNT(*) AS cnt_rows
FROM bronze.crm_cust_info
GROUP BY TRIM(cst_marital_status)
ORDER BY cnt_rows DESC;

--------------------------------------------------------------------------------------------------
-- 1.4 Check distribution of gender values
--     Justifies mapping:
--       'F' -> 'Female', 'M' -> 'Male', ELSE 'n/a'
--------------------------------------------------------------------------------------------------
SELECT 
    TRIM(cst_gndr) AS raw_gender,
    UPPER(TRIM(cst_gndr)) AS upper_trim_gender,
    COUNT(*) AS cnt_rows
FROM bronze.crm_cust_info
GROUP BY TRIM(cst_gndr), UPPER(TRIM(cst_gndr))
ORDER BY cnt_rows DESC;



/* ================================================================================================
   SILVER TABLE: silver.crm_prd_info
   SOURCE TABLE: bronze.crm_prd_info
   ================================================================================================ */

--------------------------------------------------------------------------------------------------
-- 2.1 Inspect raw product line codes
--     Justifies mapping:
--       'M' -> 'Mountain'
--       'R' -> 'Road'
--       'S' -> 'Other Sales'
--       'T' -> 'Touring'
--       ELSE 'n/a'
--------------------------------------------------------------------------------------------------
SELECT 
    TRIM(prd_line) AS raw_prd_line,
    UPPER(TRIM(prd_line)) AS upper_prd_line,
    COUNT(*) AS cnt_rows
FROM bronze.crm_prd_info
GROUP BY TRIM(prd_line), UPPER(TRIM(prd_line))
ORDER BY cnt_rows DESC;

--------------------------------------------------------------------------------------------------
-- 2.2 Validate category key extraction from prd_key
--     Used to derive:
--       cat_id  = REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')
--       prd_key = SUBSTRING(prd_key, 7, LEN(prd_key))
--------------------------------------------------------------------------------------------------
SELECT TOP (50)
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS derived_cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key))         AS derived_prd_key
FROM bronze.crm_prd_info;

--------------------------------------------------------------------------------------------------
-- 2.3 Inspect product start dates to support SCD range logic
--     In SILVER, prd_end_dt is derived using LEAD(prd_start_dt) - 1.
--------------------------------------------------------------------------------------------------
SELECT TOP (100)
    prd_id,
    prd_key,
    prd_start_dt
FROM bronze.crm_prd_info
ORDER BY prd_key, prd_start_dt;



/* ================================================================================================
   SILVER TABLE: silver.crm_sales_details
   SOURCE TABLE: bronze.crm_sales_details
   ================================================================================================ */

--------------------------------------------------------------------------------------------------
-- 3.1 Detect invalid order dates
--     Invalid when:
--       - negative
--       - not 8 digits long
--     These are set to NULL in SILVER.
--------------------------------------------------------------------------------------------------
SELECT TOP (100)
    sls_ord_num,
    sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt < 0
   OR LEN(sls_order_dt) != 8
ORDER BY sls_ord_num;

--------------------------------------------------------------------------------------------------
-- 3.2 Detect invalid ship dates
--     Invalid when:
--       - negative
--       - not 8 digits long
--       - earlier than order date
--     These are set to NULL in SILVER.
--------------------------------------------------------------------------------------------------
SELECT TOP (100)
    sls_ord_num,
    sls_order_dt,
    sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt < 0
   OR LEN(sls_ship_dt) != 8
   OR sls_ship_dt < sls_order_dt
ORDER BY sls_ord_num;

--------------------------------------------------------------------------------------------------
-- 3.3 Detect invalid due dates
--     Invalid when:
--       - negative
--       - not 8 digits long
--       - earlier than order date
--     These are set to NULL in SILVER.
--------------------------------------------------------------------------------------------------
SELECT TOP (100)
    sls_ord_num,
    sls_order_dt,
    sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt < 0
   OR LEN(sls_due_dt) != 8
   OR sls_due_dt < sls_order_dt
ORDER BY sls_ord_num;

--------------------------------------------------------------------------------------------------
-- 3.4 Detect inconsistent sales amounts
--     Cases where:
--       - sls_sales IS NULL
--       - sls_sales <= 0
--       - sls_sales != sls_quantity * ABS(sls_price)
--     Supports rule in SILVER:
--       recompute sls_sales as sls_price * sls_quantity.
--------------------------------------------------------------------------------------------------
SELECT TOP (100)
    sls_ord_num,
    sls_quantity,
    sls_price,
    sls_sales,
    sls_quantity * ABS(sls_price) AS expected_sales
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL
   OR sls_sales <= 0
   OR sls_sales != sls_quantity * ABS(sls_price)
ORDER BY sls_ord_num;



/* ================================================================================================
   SILVER TABLE: silver.erp_cust_az12
   SOURCE TABLE: bronze.erp_cust_az12
   ================================================================================================ */

--------------------------------------------------------------------------------------------------
-- 4.1 Inspect raw gender values and reveal hidden CR/LF characters
--     (These appeared as down-left arrows in the UI.)
--     Supports the cleanup using REPLACE(CHAR(13)/CHAR(10)) and TRIM in SILVER.
--------------------------------------------------------------------------------------------------
SELECT TOP (50)
    gen                      AS raw_gen,
    CONCAT('[', gen, ']')    AS gen_in_brackets,
    UPPER(gen)               AS upper_raw_gen,
    UPPER(TRIM(gen))         AS upper_trim_gen,
    LEN(gen)                 AS len_gen
FROM bronze.erp_cust_az12
WHERE gen IS NOT NULL
ORDER BY len_gen DESC;

--------------------------------------------------------------------------------------------------
-- 4.2 Detect future birthdates (bdate > GETDATE())
--     These are set to NULL in SILVER.
--------------------------------------------------------------------------------------------------
SELECT TOP (50)
    cid,
    bdate
FROM bronze.erp_cust_az12
WHERE bdate > GETDATE()
ORDER BY bdate;

--------------------------------------------------------------------------------------------------
-- 4.3 Inspect NAS-prefixed customer IDs that are later cleaned in SILVER
--     Rule: strip 'NAS' prefix and keep remainder.
--------------------------------------------------------------------------------------------------
SELECT TOP (50)
    cid,
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cleaned_cid
FROM bronze.erp_cust_az12
ORDER BY cid;



/* ================================================================================================
   SILVER TABLE: silver.erp_loc_a101
   SOURCE TABLE: bronze.erp_loc_a101
   ================================================================================================ */

--------------------------------------------------------------------------------------------------
-- 5.1 Inspect distinct raw country values
--     Used to understand the variety:
--       NULL, empty, Australia, Canada, DE, France, Germany, 
--       United Kingdom, United States, US, USA, etc.
--------------------------------------------------------------------------------------------------
SELECT 
    cntry                          AS raw_cntry,
    CONCAT('[', cntry, ']')        AS cntry_in_brackets,
    UPPER(cntry)                   AS upper_raw_cntry,
    UPPER(TRIM(cntry))             AS upper_trim_cntry,
    LEN(cntry)                     AS len_cntry
FROM bronze.erp_loc_a101
GROUP BY cntry, UPPER(cntry), UPPER(TRIM(cntry)), LEN(cntry)
ORDER BY raw_cntry;

--------------------------------------------------------------------------------------------------
-- 5.2 Preview normalized country values
--     Logic later used in SILVER:
--       - NULL / empty                 -> 'n/a'
--       - US / USA / United States     -> 'United States'
--       - DE / Germany                 -> 'Germany'
--       - Other non-empty values       -> trimmed original
--     Also strips CR/LF characters.
--------------------------------------------------------------------------------------------------
SELECT DISTINCT
    REPLACE(cid, '-', '') AS cid_cleaned,
    cntry                 AS raw_cntry,
    CASE 
        WHEN cntry IS NULL 
             OR TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')) = '' THEN 'n/a'
        WHEN UPPER(TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')))
             IN ('US', 'USA', 'UNITED STATES') THEN 'United States'
        WHEN UPPER(TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')))
             IN ('DE', 'GERMANY') THEN 'Germany'
        ELSE TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''))
    END AS normalized_cntry
FROM bronze.erp_loc_a101
ORDER BY normalized_cntry, raw_cntry;



/* ================================================================================================
   SILVER TABLE: silver.erp_px_cat_g1v2
   SOURCE TABLE: bronze.erp_px_cat_g1v2
   ================================================================================================ */

--------------------------------------------------------------------------------------------------
-- 6.1 Inspect maintenance text and reveal hidden CR/LF characters
--     These appeared as arrows in the UI and are later removed in SILVER.
--------------------------------------------------------------------------------------------------
SELECT TOP (50)
    id,
    cat,
    subcat,
    maintenance                        AS raw_maintenance,
    CONCAT('[', maintenance, ']')      AS maintenance_in_brackets,
    LEN(maintenance)                   AS len_maintenance
FROM bronze.erp_px_cat_g1v2
ORDER BY len_maintenance DESC;

--------------------------------------------------------------------------------------------------
-- 6.2 Preview cleaned maintenance values
--     SILVER logic:
--       TRIM(REPLACE(REPLACE(maintenance, CHAR(13), ''), CHAR(10), ''))
--------------------------------------------------------------------------------------------------
SELECT TOP (50)
    id,
    cat,
    subcat,
    maintenance                                       AS raw_maintenance,
    TRIM(REPLACE(REPLACE(maintenance, CHAR(13), ''), CHAR(10), '')) AS cleaned_maintenance
FROM bronze.erp_px_cat_g1v2;
