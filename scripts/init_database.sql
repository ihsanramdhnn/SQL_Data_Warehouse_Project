/*
==============================================
Create Database and Schemas
==============================================
Script Purpose:
  This script creates a new database named 'DataWarehouse' after checking if it already exists.
  If the database exists, the script will drop it and recreate the new database. 
  Additionally, the script sets up three schemas with the database: 'Bronze', 'Silver', 'Gold'.

WARNING:
  Running this script will drop the entire 'DataWarehouse' database if it exists.
  All data in the databbase will be permanently deleted. Proceed with caution and
  ensure you have proper backups before running this script.
*/

USE Master;
GO

-- Drop and recreate the 'DataWarehouse' database if it's already exist
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
  BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE:
  DROP DATABASE DataWarehouse;
END;
GO

-- Create the "DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

Use DataWarehouse:
GO

-- Create Schemas

CREATE SCHEMA Bronze;
GO

CREATE SCHEMA SILVER;
GO

CREATE SCHEMA GOLD;
GO
