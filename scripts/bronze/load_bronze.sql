CREATE OR ALTER PROCEDURE bronze.load_bronze AS

-- exec bronze.load_bronze

BEGIN
	/*
		This script will do an initial data load into the DataWarehouse database

		WARNING:
			This will completely refresh the database - make sure any required backups have been done prior to running this script!!

	*/

	IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
		TRUNCATE TABLE bronze.crm_cust_info;

	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Udemy\The Complete SQL Bootcamp\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	select 'bronze.crm_cust_info' as 'TABLE', count(*) as count from bronze.crm_cust_info;


	-----

	IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
		TRUNCATE TABLE bronze.crm_prd_info;

	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Udemy\The Complete SQL Bootcamp\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	select 'bronze.crm_prd_info' as 'TABLE', count(*) as count from bronze.crm_prd_info;
	

	---

	IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
		TRUNCATE TABLE bronze.crm_sales_details;


	BULK INSERT bronze.crm_sales_details FROM 'C:\Udemy\The Complete SQL Bootcamp\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);


	select 'bronze.crm_sales_details' as 'TABLE', count(*) as count from bronze.crm_sales_details;


	---

	IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
		TRUNCATE TABLE bronze.erp_cust_az12;

	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Udemy\The Complete SQL Bootcamp\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	select 'bronze.erp_cust_az12' as 'TABLE', count(*) as count from bronze.erp_cust_az12;
 

	---

	IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
		TRUNCATE TABLE bronze.erp_loc_a101;

	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Udemy\The Complete SQL Bootcamp\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);


	select 'bronze.erp_loc_a101' as 'TABLE', count(*) as count from bronze.erp_loc_a101;


	---

	IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\Udemy\The Complete SQL Bootcamp\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	select 'bronze.erp_px_cat_g1v2' as 'TABLE', count(*) as count from bronze.erp_px_cat_g1v2


END
