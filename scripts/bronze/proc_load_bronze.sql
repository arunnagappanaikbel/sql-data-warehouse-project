/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
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
	RAISE NOTICE 'Loading Bronze Layer';
	RAISE NOTICE '================================================';

	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '------------------------------------------------';
	
	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
	RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
    COPY bronze.crm_cust_info
    FROM 'D:\Data Engineering\SQL Projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );

	PERFORM pg_sleep(1.000);
	end_time := clock_timestamp();
	duration = end_time - start_time;
	RAISE NOTICE '>> Load Duration for load customer data is: %', duration;
	RAISE NOTICE '>> ======================================================================================================';	

	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
	RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
    COPY bronze.crm_prd_info
    FROM 'D:\Data Engineering\SQL Projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );

	PERFORM pg_sleep(1.000);
	end_time := clock_timestamp();
	duration = end_time - start_time;
	RAISE NOTICE '>> Load Duration for loading product data is: %', duration;
	RAISE NOTICE '>> ======================================================================================================';	

	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
	RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
    COPY bronze.crm_sales_details
    FROM 'D:\Data Engineering\SQL Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );

	PERFORM pg_sleep(1.000);
	end_time := clock_timestamp();
	duration = end_time - start_time;
	RAISE NOTICE '>> Load Duration for loading sales data is: %', duration;
	RAISE NOTICE '>> ======================================================================================================';

	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE 'Loading ERP Tables';
	RAISE NOTICE '------------------------------------------------';
	
	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
	RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101
    FROM 'D:\Data Engineering\SQL Projects\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );

	PERFORM pg_sleep(1.000);
	end_time := clock_timestamp();
	duration = end_time - start_time;
	RAISE NOTICE '>> Load Duration for loading loc_a101 data is: %', duration;
	RAISE NOTICE '>> ======================================================================================================';

	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
	RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12
    FROM 'D:\Data Engineering\SQL Projects\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );

	PERFORM pg_sleep(1.000);
	end_time := clock_timestamp();
	duration = end_time - start_time;
	RAISE NOTICE '>> Load Duration for loading erp_cust_az12 data is: %', duration;
	RAISE NOTICE '>> ======================================================================================================';

	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2
    FROM 'D:\Data Engineering\SQL Projects\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );

	PERFORM pg_sleep(1.000);
	end_time := clock_timestamp();
	duration = end_time - start_time;
	RAISE NOTICE '>> Load Duration for loading px_cat_g1v2 data is: %', duration;
	RAISE NOTICE '>> ======================================================================================================';

	RAISE NOTICE '>> Loading bronze layer data is completed';

	batch_end_time := clock_timestamp();
	duration = batch_end_time - batch_start_time;
	RAISE NOTICE '>> bronze layer batch duration is: %', duration;

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error occurred: %', SQLERRM;
END
$$;


CALL bronze.load_bronze();
