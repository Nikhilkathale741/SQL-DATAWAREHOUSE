/*
============================================================
Create Database and Schemas
============================================================

Script Purpose:
    This script creates a new database named 'DataWarehouse'.
    After creating the database, it initializes three schemas
    to support a Medallion Architecture:
        - bronze  : Raw / source data
        - silver  : Cleaned and transformed data
        - gold    : Analytics-ready data

WARNING:
    Running this script may overwrite existing objects if
    executed on an existing database.
    Ensure you have proper backups before running this script.
============================================================
*/

USE master;
GO

-- Create the DataWarehouse database
CREATE DATABASE DataWarehouse;
GO

-- Switch context to the new database
USE DataWarehouse;
GO

-- Create schemas for Medallion Architecture
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
