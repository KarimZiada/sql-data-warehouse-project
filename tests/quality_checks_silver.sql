/* =================================================================================================
   Data Quality Checks for Silver Layer Design
   =================================================================================================
   This script contains the exploratory / diagnostic SQL queries that were used to identify 
   bad or inconsistent data patterns in the BRONZE layer and to derive the cleansing rules 
   implemented in [silver.load_silver].

   Main goals:
   - Detect duplicate business keys (e.g., customers with multiple records).
   - Identify invalid or suspicious dates (future dates, malformed integer dates, etc.).
   - Standardize categorical fields (gender, marital status, product line, country, etc.).
   - Remove hidden control characters (CR/LF) that appeared as arrows in the UI.
   - Validate sales amounts vs. price * quantity.

   All queries below are READ-ONLY diagnostics. The actual cleansing logic is implemented in:
   - PROCEDURE [silver].[load_silver]

   Usage:
   - Run these queries against the BRONZE layer to understand raw data issues.
   - Compare results with the SILVER tables loaded by [silver.load_silver] to verify that 
     the cleansing rules behave as expected.
================================================================================================= */

--------------------------------------------------------------------------------------------------
-- 1. BRONZE.CRM_CUST_INFO – Duplicate Customers & Attribute Profiling
--------------------------------------------------------------------------------------------------

-- 1.1 Detect customers with duplicate cst_id (business key duplicates)
SELECT 
    cst_id,
    COUNT(*) AS cnt_rows
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1
ORDER BY cnt_rows DESC, cst_id;

-- 1.2 Inspect all duplicate rows per customer, ordered by cst_create_date (latest first)
--     This supports the decision to keep only the latest record per cst_id in SILVER.
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

-- 1.3 Check distribution of marital status values (to justify mapping S/M -> Single/Married, else 'n/a')
SELECT 
    TRIM(cst_marital_status) AS raw_marital_status,
    COUNT(*) AS cnt_rows
FROM bronze.crm_cust_info
GROUP BY TRIM(cst_marital_status)
ORDER BY cnt_rows DESC;

-- 1.4 Check distribution of gender values (to justify mapping F/M -> Female/Male, else 'n/a')
SELECT 
    TRIM(cst_gndr) AS raw_gender,
    UPPER(TRIM(cst_gndr)) AS upper_trim_gender,
    COUNT(*) AS cnt_rows
FROM bronze.crm_cust_info
GROUP BY TRIM(cst_gndr), UPPER(TRIM(cst_gndr))
ORDER BY cnt_rows DESC;


--------------------------------------------------------------------------------------------------
-- 2. BRONZE.CRM_PRD_INFO – Product Line, Category & SCD Range Logic
--------------------------------------------------------------------------------------------------

-- 2.1 Inspect raw product line codes to justify mapping:
--     'M' -> Mountain, 'R' -> Road, 'S' -> Other Sales, 'T' -> Touring, else 'n/a'
SELECT 
    TRIM(prd_line) AS raw_prd_line,
    UPPER(TRIM(prd_line)) AS upper_prd_line,
    COUNT(*) AS cnt_rows
FROM bronze.crm_prd_info
GROUP BY TRIM(prd_line), UPPER(TRIM(prd_line))
ORDER BY cnt_rows DESC;

-- 2.2 Validate category key extraction from prd_key (first 5 chars) and product key (rest)
--     Used to derive cat_id and cleaned prd_key in SILVER.
SELECT TOP (50)
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS derived_cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key))         AS derived_prd_key
FROM bronze.crm_prd_info;

-- 2.3 Inspect product date ranges to support SCD logic (prd_start_dt, prd_end_dt in SILVER)
SELECT TOP (100)
    prd_id,
    prd_key,
    prd_start_dt
FROM bronze.crm_prd_info
ORDER BY prd_key, prd_start_dt;


--------------------------------------------------------------------------------------------------
-- 3. BRONZE.CRM_SALES_DETAILS – Date Quality & Sales Consistency
--------------------------------------------------------------------------------------------------

-- 3.1 Detect invalid order dates: negative or not 8-digit integers
SELECT TOP (100)
    sls_ord_num,
    sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt < 0
   OR LEN(sls_order_dt) != 8
ORDER BY sls_ord_num;

-- 3.2 Detect invalid ship dates: negative, wrong length, or earlier than order date
SELECT TOP (100)
    sls_ord_num,
    sls_order_dt,
    sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt < 0
   OR LEN(sls_ship_dt) != 8
   OR sls_ship_dt < sls_order_dt
ORDER BY sls_ord_num;

-- 3.3 Detect invalid due dates: negative, wrong length, or earlier than order date
SELECT TOP (100)
    sls_ord_num,
    sls_order_dt,
    sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt < 0
   OR LEN(sls_due_dt) != 8
   OR sls_due_dt < sls_order_dt
ORDER BY sls_ord_num;

-- 3.4 Detect inconsistent sales amounts where:
--     - sales <= 0
--     - sales is NULL
--     - sales != quantity * ABS(price)
--     This supports the rule: recompute sls_sales as sls_price * sls_quantity in SILVER.
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


--------------------------------------------------------------------------------------------------
-- 4. BRONZE.ERP_CUST_AZ12 – Customer ID, Future Birthdates & Gender Cleanup
--------------------------------------------------------------------------------------------------

-- 4.1 Inspect raw gender values and show impact of trimming / uppercasing
--     (Also useful for detecting hidden CR/LF characters that showed as arrows in the UI.)
SELECT TOP (50)
    gen                  AS raw_gen,
    CONCAT('[', gen, ']') AS gen_in_brackets,
    UPPER(gen)           AS upper_raw_gen,
    UPPER(TRIM(gen))     AS upper_trim_gen,
    LEN(gen)             AS len_gen
FROM bronze.erp_cust_az12
WHERE gen IS NOT NULL
ORDER BY len_gen DESC;

-- 4.2 Detect future birthdates (bdate > GETDATE()), which are later set to NULL in SILVER.
SELECT TOP (50)
    cid,
    bdate
FROM bronze.erp_cust_az12
WHERE bdate > GETDATE()
ORDER BY bdate;

-- 4.3 Inspect NAS-prefixed customer IDs that are later cleaned (strip NAS prefix).
SELECT TOP (50)
    cid,
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cleaned_cid
FROM bronze.erp_cust_az12
ORDER BY cid;


--------------------------------------------------------------------------------------------------
-- 5. BRONZE.ERP_LOC_A101 – Country Normalization & Hidden Characters
--------------------------------------------------------------------------------------------------

-- 5.1 Inspect distinct raw country values to understand the variety:
--     null, empty, Australia, Canada, DE, France, Germany, United Kingdom, United States, US, USA, etc.
SELECT 
    cntry                          AS raw_cntry,
    CONCAT('[', cntry, ']')        AS cntry_in_brackets,
    UPPER(cntry)                   AS upper_raw_cntry,
    UPPER(TRIM(cntry))             AS upper_trim_cntry,
    LEN(cntry)                     AS len_cntry
FROM bronze.erp_loc_a101
GROUP BY cntry, UPPER(cntry), UPPER(TRIM(cntry)), LEN(cntry)
ORDER BY raw_cntry;

-- 5.2 Preview normalized country values (logic later used in SILVER):
--     - NULL / empty -> 'n/a'
--     - US / USA / United States -> 'United States'
--     - DE / Germany             -> 'Germany'
--     - other non-empty values   -> trimmed original
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


--------------------------------------------------------------------------------------------------
-- 6. BRONZE.ERP_PX_CAT_G1V2 – Maintenance Field Cleanup (Hidden CR/LF)
--------------------------------------------------------------------------------------------------

-- 6.1 Inspect maintenance text to reveal hidden CR/LF (they appeared as arrows in the UI).
SELECT TOP (50)
    id,
    cat,
    subcat,
    maintenance                        AS raw_maintenance,
    CONCAT('[', maintenance, ']')      AS maintenance_in_brackets,
    LEN(maintenance)                   AS len_maintenance
FROM bronze.erp_px_cat_g1v2
ORDER BY len_maintenance DESC;

-- 6.2 Preview cleaned maintenance values (logic later used in SILVER):
--     Strip CHAR(13) and CHAR(10) and TRIM spaces.
SELECT TOP (50)
    id,
    cat,
    subcat,
    maintenance                                       AS raw_maintenance,
    TRIM(REPLACE(REPLACE(maintenance, CHAR(13), ''), CHAR(10), '')) AS cleaned_maintenance
FROM bronze.erp_px_cat_g1v2;
