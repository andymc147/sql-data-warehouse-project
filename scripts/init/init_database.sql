-- Create Database 'DataWarehouse'
/*
======================================================================================
Create Database and Schemas
======================================================================================
Script Purpose
	Thise script creates a new database named 'DataWarehouse after checking if it already exists.
	If the database exists it is dropped and recreated. Additionally, the script sets up 3 schemas
	within the database: 'bronze', 'silver', and 'gold'.

WARNING:
	Running this script will drop the entire 'DataWarehouse' database if it exists.
	All data in the database will be permanently deleted. Proceed with caution
	and ensure you have the proper backups before running this script!!
*/

use master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (Select 1 FROM sys.databases WHERE name = 'DataWarehouse')
	BEGIN
		ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
		DROP DATABASE  ataWarehouse;
	END;
GO
-- Create the database 'DataWarehouse'
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
