/*
===============================================================================
Stored Procedure: Load Silver Layer (Source -> Bronze)
===============================================================================
Script Purpose:
	This stored procedure transform the bronze layer all table
    This stored procedure loads data into the 'silver' from bronze 
    It performs the following actions:

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
	duration INTERVAL;
	batch_start_time TIMESTAMP;
	batch_end_time TIMESTAMP;
BEGIN
	batch_start_time := clock_timestamp();
    RAISE NOTICE '================================================';
	RAISE NOTICE 'Loading Silver Layer';
	RAISE NOTICE '================================================';

	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '------------------------------------------------';
	
	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
	RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_martial_status,
	cst_gender,
	cst_create_date
	)
	with GET_ROWNUM AS (
	SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as rank
	FROM bronze.crm_cust_info)
	SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) as cst_firstname,
	TRIM(cst_lastname) as cst_lastname,
	CASE WHEN UPPER(TRIM(cst_gender)) = 'S' THEN 'Single'
		 WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Married'
		 ELSE 'n/a'
	END cst_martial_status,
	CASE WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
		 WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
		 ELSE 'n/a'
	END cst_gender,
	cst_create_date
	FROM GET_ROWNUM
	WHERE rank = 1
	ORDER BY cst_id;
	PERFORM pg_sleep(1.000);
	end_time := clock_timestamp();
	duration = end_time - start_time;
	RAISE NOTICE '>> Load Duration for load customer data is: %', duration;
	RAISE NOTICE '>> ======================================================================================================';	

	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
	RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	)
	SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') as cat_id,
	SUBSTRING(prd_key, 7, LENGTH(prd_key)) as prd_key,
	prd_nm,
	COALESCE(prd_cost, '0') as prd_cost,
	CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Montain'
		 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other sales'
		 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		 ELSE 'n/a'
	END AS prd_line,
	prd_start_dt,
	LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt )-1 as prd_end_dt
	FROM bronze.crm_prd_info;
	PERFORM pg_sleep(1.000);
	end_time := clock_timestamp();
	duration = end_time - start_time;
	RAISE NOTICE '>> Load Duration for loading product data is: %', duration;
	RAISE NOTICE '>> ======================================================================================================';	
	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
	RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
    INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
	SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
		 ELSE TO_DATE(sls_order_dt :: TEXT,'YYYYMMDD')
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
		 ELSE TO_DATE(sls_ship_dt :: TEXT,'YYYYMMDD')
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
		 ELSE TO_DATE(sls_due_dt :: TEXT,'YYYYMMDD')
	END AS sls_due_dt,
	CASE WHEN sls_sales is NULL OR sls_sales<=0 OR (sls_sales != sls_quantity * ABS(sls_price)) THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE WHEN sls_price is NULL OR sls_price<=0 THEN  ABS(sls_price)/sls_quantity 
		 ELSE sls_price
	END AS sls_price
	FROM 
	bronze.crm_sales_details;
	PERFORM pg_sleep(1.000);
	end_time := clock_timestamp();
	duration = end_time - start_time;
	RAISE NOTICE '>> Load Duration for loading sales data is: %', duration;
	RAISE NOTICE '>> ======================================================================================================';

	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading ERP Tables';
	RAISE NOTICE '------------------------------------------------';
	
	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
	RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';
    INSERT INTO silver.erp_cust_az12(
	cid,
	bdate,
	gen)
	SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
		 ELSE cid
	END AS cid,
	CASE WHEN bdate > CURRENT_DATE THEN NULL
		 ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
		 WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
		 ELSE 'n/a'
	END AS gen
	FROM bronze.erp_cust_az12;

	PERFORM pg_sleep(1.000);
	end_time := clock_timestamp();
	duration = end_time - start_time;
	RAISE NOTICE '>> Load Duration for loading loc_a101 data is: %', duration;
	RAISE NOTICE '>> ======================================================================================================';

	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
	RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
    INSERT INTO silver.erp_loc_a101(
	cid,
	cntry)
	SELECT
	REPLACE(cid,'-','') AS cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US','USA') THEN 'United State'
		 WHEN TRIM(cntry) ='' OR cntry IS NULL THEN 'n/a'
		 ELSE TRIM(cntry)
	END AS cntry
	FROM 
	bronze.erp_loc_a101;

	PERFORM pg_sleep(1.000);
	end_time := clock_timestamp();
	duration = end_time - start_time;
	RAISE NOTICE '>> Load Duration for loading erp_cust_az12 data is: %', duration;
	RAISE NOTICE '>> ======================================================================================================';

	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
	RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance
	)
	SELECT
	id,
	cat,
	subcat,
	maintenance
	FROM 
	bronze.erp_px_cat_g1v2;
	PERFORM pg_sleep(1.000);
	end_time := clock_timestamp();
	duration = end_time - start_time;
	RAISE NOTICE '>> Load Duration for loading px_cat_g1v2 data is: %', duration;
	RAISE NOTICE '>> ======================================================================================================';

	RAISE NOTICE '>> Loading silver layer data is completed';

	batch_end_time := clock_timestamp();
	duration = batch_end_time - batch_start_time;
	RAISE NOTICE '>> silver layer batch duration is: %', duration;

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END
$$;


CALL silver.load_silver();
