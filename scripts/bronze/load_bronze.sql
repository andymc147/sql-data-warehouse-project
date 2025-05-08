/*
============================================================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
============================================================================================================================
Script Purpose:
	This stored procedure loads data into the 'bronze' schema from external csv files.
	It performs the following actions:
	- Truncates the bronze tables before loading data
	- Uses the 'BULK INSERT' command to load data from csv files to bronze tables.

Parameters:
	None.
	This stored procedure does not accept any parameters or return any values.

Usage Example:
	EXEC bronze.load_bronze

*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS

-- Command to run this procedure
-- exec bronze.load_bronze

BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, 
			@job_start_time DATETIME, @job_end_time DATETIME
	BEGIN TRY
		SET @job_start_time = GETDATE();
		SET @start_time = GETDATE();
		PRINT '=================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=================================';

		PRINT '---------------------------------';
		PRINT 'Loading CRM tables';
		PRINT '---------------------------------';


		/*
			This script will do an initial data load into the DataWarehouse database

			WARNING:
				This will completely refresh the database - make sure any required backups have been done prior to running this script!!

		*/
		PRINT '>> TRUNCATING Table: bronze.crm_cust_info';
		IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
			TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> LOADING Table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Udemy\The Complete SQL Bootcamp\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		select 'bronze.crm_cust_info' as 'TABLE', count(*) as count from bronze.crm_cust_info;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------';
		-----
		set @start_time = GETDATE();
		PRINT '>> TRUNCATING Table: bronze.crm_prd_info';
		IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
			TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> LOADING Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Udemy\The Complete SQL Bootcamp\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		select 'bronze.crm_prd_info' as 'TABLE', count(*) as count from bronze.crm_prd_info;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------';
		-----
		set @start_time = GETDATE();

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------';
		-----
		set @start_time = GETDATE();
		PRINT '>> TRUNCATING Table: crm_sales_details';
		IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
			TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> LOADING Table: crm_sales_details';
		BULK INSERT bronze.crm_sales_details FROM 'C:\Udemy\The Complete SQL Bootcamp\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);


		select 'bronze.crm_sales_details' as 'TABLE', count(*) as count from bronze.crm_sales_details;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------';
		-----
		set @start_time = GETDATE();


		---
		PRINT '---------------------------------';
		PRINT 'Loading ERP tables';
		PRINT '---------------------------------';

		PRINT '>> TRUNCATING Table: erp_cust_az12';
		IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
			TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> LOADING Table: erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Udemy\The Complete SQL Bootcamp\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		select 'bronze.erp_cust_az12' as 'TABLE', count(*) as count from bronze.erp_cust_az12;
 
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------';
	
		set @start_time = GETDATE();
		PRINT '>> TRUNCATING Table: erp_loc_a101';
		IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
			TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> LOADING Table: erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Udemy\The Complete SQL Bootcamp\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);


		select 'bronze.erp_loc_a101' as 'TABLE', count(*) as count from bronze.erp_loc_a101;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------';
	
		set @start_time = GETDATE();
		PRINT '>> TRUNCATING Table: erp_px_cat_g1v2';
		IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
			TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> LOADING Table: erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Udemy\The Complete SQL Bootcamp\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		select 'bronze.erp_px_cat_g1v2' as 'TABLE', count(*) as count from bronze.erp_px_cat_g1v2

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ------------------';
		-----
		set @start_time = GETDATE();

		SET @job_end_time = GETDATE();
		PRINT '=================================';
		PRINT ' Loading Bronze Layer is Complete';
		PRINT ' Total Load Duration: ' + CAST(DATEDIFF(second, @job_start_time, @job_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=================================';
		
	END TRY
	BEGIN CATCH
		PRINT '==========================================';
		PRINT 'ERROR OCCURRED DURING LOAD OF BRONZE LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Message: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================';

	END CATCH

END

