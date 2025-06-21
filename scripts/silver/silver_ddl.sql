
DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE IF NOT EXISTS silver.crm_cust_info (
	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_martial_status VARCHAR(10),
	cst_gender VARCHAR(10),
	cst_create_date DATE,
	dwh_created_date DATE DEFAULT NOW()
);

DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE IF NOT EXISTS silver.crm_prd_info(
prd_id INTEGER,
cat_id VARCHAR(50),
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost VARCHAR(50),
prd_line VARCHAR(50),
prd_start_dt DATE,	
prd_end_dt DATE,
dwh_created_date DATE DEFAULT NOW()
);

DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE IF NOT EXISTS silver.crm_sales_details(
sls_ord_num VARCHAR(50),
sls_prd_key	VARCHAR(50),
sls_cust_id INTEGER,
sls_order_dt DATE,
sls_ship_dt	DATE,
sls_due_dt	DATE,
sls_sales	INTEGER,
sls_quantity INTEGER,
sls_price INTEGER,
dwh_created_date DATE DEFAULT NOW()
);

DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE IF NOT EXISTS silver.erp_loc_a101(
cid varchar(50),
cntry varchar(50),
dwh_created_date DATE DEFAULT NOW()
);

DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE IF NOT EXISTS silver.erp_cust_az12(
cid varchar(50),
bdate date,
gen varchar(50),
dwh_created_date DATE DEFAULT NOW()
);

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE IF NOT EXISTS silver.erp_px_cat_g1v2(
id varchar(50),
cat varchar(50),
subcat varchar(50),
maintenance varchar(50),
dwh_created_date DATE DEFAULT NOW()
);






