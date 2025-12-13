/*
================================================================
Create Database and Schemas
================================================================
Script Purpose:
	This script creates a new database named 'fuzzy_factory' after checking if it already exists.
	If the database exists, it is dropped and recreated. additionally, the script sets up three
	schemas within the database: 'bronze', 'silver', and 'gold'.

WARNING:
	Running this script will drop the entire 'fuzzy_factory' database if it exists.
	all data in the database will be permanently deleted. Proceed with caution and
	ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'fuzzy_factory' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'fuzzy_factory')
BEGIN
	ALTER DATABASE fuzzy_factory SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE fuzzy_factory;
END;
GO

-- Create the 'fuzzy_factory' database	
CREATE DATABASE fuzzy_factory;
GO
	
USE fuzzy_factory;
GO

-- Create Schemas	
CREATE SCHEMA bronze;
GO
	
CREATE SCHEMA silver;
GO
	
CREATE SCHEMA gold;
GO
