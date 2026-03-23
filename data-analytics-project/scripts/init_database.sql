
/* Create Database and Schemas. 
Drop and recreate the 'DataWarehouse' database*/
DROP DATABASE IF EXISTS DataWarehouse;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO