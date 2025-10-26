/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- Switch to the master database to manage databases
USE master;
GO

-- Check if the 'DataWarehouse' database already exists, and if so, drop it
-- This ensures we start with a fresh database for loading new data
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    -- Set database to single-user mode and rollback any existing connections
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    -- Delete 'DataWarehouse' to remove old data and structure
    DROP DATABASE DataWarehouse;
END;
GO

-- Create a new, empty 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

-- Switch to the new 'DataWarehouse' database for further setup
USE DataWarehouse;
GO

-- Create 'bronze' schema: for raw, ingested data (first ETL layer)
CREATE SCHEMA bronze;
GO

-- Create 'silver' schema: for cleaned and refined data (second ETL layer)
CREATE SCHEMA silver;
GO

-- Create 'gold' schema: for final, consumable data used in analytics/reporting
CREATE SCHEMA gold;
GO
