-- Schema
 
IF NOT EXISTS (SELECT * FROM sys.schemas where name = 'bronze')
BEGIN
-- Create the schema if it does not exist
  EXEC('CREATE SCHEMA [bronze]')
END
 
 
IF NOT EXISTS (SELECT * FROM sys.schemas where name = 'silver')
BEGIN
-- Create the schema if it does not exist
  EXEC('CREATE SCHEMA [silver]')
END
 
 
IF NOT EXISTS (SELECT * FROM sys.schemas where name = 'gold')
BEGIN
-- Create the schema if it does not exist
  EXEC('CREATE SCHEMA [gold]')
END
 
 
--
 
 
-- Broze objects
 
 
IF EXISTS (SELECT * FROM sys.views where name = 'company' and SCHEMA_ID ('bronze') = schema_id)
BEGIN
  DROP VIEW [bronze].[company]
END
 
GO
 
 
CREATE VIEW bronze.company
AS
SELECT      [ShortName],
            [LongName],
            [Symbol],
            [Address],
            [City],
            [Zip],
            [State],
            [Country],
            [Website],
            [Industry],
            [Sector],
            [Currency],
            [LogoURL],
            [FetchDataTimestamp],
            [StockMarket]
FROM [LH_Stocks].[dbo].[bronze_company]
 
GO
 
--
 
-- Silver objects
 
IF EXISTS (SELECT * FROM sys.tables where name = 'company' and SCHEMA_ID ('silver') = schema_id)
BEGIN
  DROP TABLE [silver].[company]
END
 
GO
 
CREATE TABLE [silver].[company]
(
    [ShortName] [varchar](128) NULL,
    [LongName] [varchar](512) NULL,
    [Symbol] [varchar](128) NULL,
    [Address] [varchar](128) NULL,
    [City] [varchar](128) NULL,
    [Zip] [varchar](128) NULL,
    [State] [varchar](128) NULL,
    [Country] [varchar](128) NULL,
    [Website] [varchar](128) NULL,
    [Industry] [varchar](128) NULL,
    [Sector] [varchar](128) NULL,
    [Currency] [varchar](32) NULL,
    [LogoURL] [varchar](128) NULL,
    [StockMarket] [varchar](128) NULL,
    [FetchDataTimestamp] [datetime2](6) NULL,
    [Fingerprint] [VARBINARY](8000) NULL
)
GO
 
 
IF EXISTS (SELECT * FROM sys.views where name = 'company_data' and SCHEMA_ID ('silver') = schema_id)
BEGIN
  DROP VIEW [silver].[company_data]
END
GO
 
CREATE VIEW [silver].[company_data]
AS
 
SELECT  [ShortName],
        [LongName],
        [Symbol],
        [Address],
        [City],
        [Zip],
        [State],
        [Country],
        [Website],
        [Industry],
        [Sector],
        [Currency],
        [LogoURL],
        [StockMarket],
        [FetchDataTimestamp],
        CONVERT([VARBINARY](8000),
        HASHBYTES('SHA2_256',
          CONCAT(
            [LongName],
            [Symbol],
            [Address],
            [City],
            [Zip],
            [State],
            [Country],
            [Website],
            [Industry],
            [Sector],
            [Currency],
            [LogoURL],
            [StockMarket]
               )
         )) AS Fingerprint
FROM bronze.company
 
GO
 
IF EXISTS (SELECT * FROM sys.views where name = 'company_latest' and SCHEMA_ID ('silver') = schema_id)
BEGIN
  DROP VIEW [silver].[company_latest]
END
GO
 
CREATE VIEW [silver].[company_latest]
AS
 
WITH
silver_company_latest_version
AS
(
  SELECT MAX([FetchDataTimestamp]) AS max_FetchDataTimestamp
        ,[Symbol]
  FROM [silver].[company]
  GROUP BY [Symbol]
)
SELECT silver_company.*
FROM [silver].[company] as silver_company
  INNER JOIN silver_company_latest_version
     ON silver_company.[Symbol] = silver_company_latest_version.[Symbol]
    AND silver_company.[FetchDataTimestamp] = max_FetchDataTimestamp
 
GO
 
IF EXISTS (SELECT * FROM sys.procedures where name = 'CreateVersionCompany' and SCHEMA_ID ('silver') = schema_id)
BEGIN
  DROP PROCEDURE [silver].[CreateVersionCompany]
END
GO
 
 
CREATE PROCEDURE [silver].[CreateVersionCompany]
AS
 
INSERT INTO [silver].[company]
-- Todo compare with last known version
SELECT  [company_data].[ShortName],
        [company_data].[LongName],
        [company_data].[Symbol],
        [company_data].[Address],
        [company_data].[City],
        [company_data].[Zip],
        [company_data].[State],
        [company_data].[Country],
        [company_data].[Website],
        [company_data].[Industry],
        [company_data].[Sector],
        [company_data].[Currency],
        [company_data].[LogoURL],
        [company_data].[StockMarket],
        [company_data].[FetchDataTimestamp],
        [company_data].[Fingerprint]
FROM [silver].[company_data]
  LEFT OUTER JOIN [silver].[company_latest]
   ON [company_data].[Symbol] = [company_latest].[Symbol]
   -- Only include records where the Fingerprint from company_data
   -- does not match the Fingerprint from the company table
WHERE [company_data].[Fingerprint] <> ISNULL([company_latest].[Fingerprint], CONVERT(VARBINARY(8000), ''))
 
GO
 
-- Gold
 
IF EXISTS (SELECT * FROM sys.tables where name = 'company_data_to_process_data' and SCHEMA_ID ('gold') = schema_id)
BEGIN
  DROP TABLE [gold].[company_data_to_process_data]
END
 
 
CREATE TABLE [WH_Stocks].[gold].[company_data_to_process_data]
(
    [company_bk] [varchar](128) NULL,
    [ShortName] [varchar](128) NULL,
    [LongName] [varchar](512) NULL,
    [Symbol] [varchar](128) NULL,
    [Address] [varchar](128) NULL,
    [City] [varchar](128) NULL,
    [Zip] [varchar](128) NULL,
    [State] [varchar](128) NULL,
    [Country] [varchar](128) NULL,
    [Website] [varchar](128) NULL,
    [Industry] [varchar](128) NULL,
    [Sector] [varchar](128) NULL,
    [Currency] [varchar](32) NULL,
    [LogoURL] [varchar](128) NULL,
    [StockMarket] [varchar](128) NULL,
    [FetchDataTimestamp] [datetime2](6) NULL,
    [Fingerprint] [varbinary](8000) NULL
)
GO
 
 
IF EXISTS (SELECT * FROM sys.tables where name = 'company' and SCHEMA_ID ('gold') = schema_id)
BEGIN
  DROP TABLE [gold].[company]
END
 
 
CREATE TABLE [WH_Stocks].[gold].[company]
(
    [company_id] [int] NOT NULL,
    [company_bk] [varchar](128) NOT NULL,
    [ShortName] [varchar](128) NULL,
    [LongName] [varchar](512) NULL,
    [Symbol] [varchar](128) NULL,
    [Address] [varchar](128) NULL,
    [City] [varchar](128) NULL,
    [Zip] [varchar](128) NULL,
    [State] [varchar](128) NULL,
    [Country] [varchar](128) NULL,
    [Website] [varchar](128) NULL,
    [Industry] [varchar](128) NULL,
    [Sector] [varchar](128) NULL,
    [Currency] [varchar](32) NULL,
    [LogoURL] [varchar](128) NULL,
    [StockMarket] [varchar](128) NULL,
    [FetchDataTimestamp] [datetime2](6) NULL,
    [Fingerprint] [varbinary](8000) NOT NULL,
    [Valid_From] [datetime2](6) NOT NULL,
    [Valid_To]   [datetime2](6) NOT NULL,
    [IsCurrent] [char](1) NOT NULL
)
 
GO
 
 
IF EXISTS (SELECT * FROM sys.tables where name = 'company_current' and SCHEMA_ID ('gold') = schema_id)
BEGIN
  DROP TABLE [gold].[company_current]
END
 
 
CREATE TABLE [WH_Stocks].[gold].[company_current]
(
    [company_id] [int] NOT NULL,
    [company_bk] [varchar](128) NOT NULL,
    [ShortName] [varchar](128) NULL,
    [LongName] [varchar](512) NULL,
    [Symbol] [varchar](128) NULL,
    [Address] [varchar](128) NULL,
    [City] [varchar](128) NULL,
    [Zip] [varchar](128) NULL,
    [State] [varchar](128) NULL,
    [Country] [varchar](128) NULL,
    [Website] [varchar](128) NULL,
    [Industry] [varchar](128) NULL,
    [Sector] [varchar](128) NULL,
    [Currency] [varchar](32) NULL,
    [LogoURL] [varchar](128) NULL,
    [StockMarket] [varchar](128) NULL,
    [FetchDataTimestamp] [datetime2](6) NULL,
    [Fingerprint] [varbinary](8000) NOT NULL,
    [Valid_From] [datetime2](6) NOT NULL,
    [Valid_To]   [datetime2](6) NOT NULL,
    [IsCurrent] [char](1) NOT NULL
)
 
GO
 
 
 
IF EXISTS (SELECT * FROM sys.views where name = 'company_data' and SCHEMA_ID ('gold') = schema_id)
BEGIN
  DROP VIEW [gold].[company_data]
END
GO
 
CREATE VIEW [gold].[company_data]
AS
 
SELECT    
            [Symbol]   AS [company_bk],
            [ShortName],
            [LongName],
            [Symbol],
            [Address],
            [City],
            [Zip],
            [State],
            [Country],
            [Website],
            [Industry],
            [Sector],
            [Currency],
            [LogoURL],
            [StockMarket],
            [FetchDataTimestamp],
            [Fingerprint]
FROM [silver].[company_latest]
 
GO
 
 
IF EXISTS (SELECT * FROM sys.views where name = 'company_data_to_process' and SCHEMA_ID ('gold') = schema_id)
BEGIN
  DROP VIEW [gold].[company_data_to_process]
END
GO
 
 
CREATE VIEW [gold].[company_data_to_process]
AS
 
SELECT      [company_data].[company_bk],
            [company_data].[ShortName],
            [company_data].[LongName],
            [company_data].[Symbol],
            [company_data].[Address],
            [company_data].[City],
            [company_data].[Zip],
            [company_data].[State],
            [company_data].[Country],
            [company_data].[Website],
            [company_data].[Industry],
            [company_data].[Sector],
            [company_data].[Currency],
            [company_data].[LogoURL],
            [company_data].[StockMarket],
            [company_data].[FetchDataTimestamp],
            [company_data].[Fingerprint]
FROM [gold].[company_data]
  LEFT OUTER JOIN [gold].[company]
    ON [gold].[company_data].[company_bk] = [gold].[company].[company_bk]
WHERE [company_data].[Fingerprint] <> ISNULL([company].[Fingerprint], CONVERT(VARBINARY(8000), ''))
  AND (([company].[IsCurrent] IS NULL) OR ([company].[IsCurrent] = 1))
 
GO
 
--
IF EXISTS (SELECT * FROM sys.procedures where name = 'CreateSCD2Company' and SCHEMA_ID ('gold') = schema_id)
BEGIN
  DROP PROCEDURE [gold].[CreateSCD2Company]
END
GO
 
 
 
CREATE PROCEDURE [gold].[CreateSCD2Company]
AS
 
 
-- Cache to rows which must be processed
TRUNCATE TABLE [gold].[company_data_to_process_data]
 
INSERT
INTO [gold].[company_data_to_process_data]
SELECT *
FROM [gold].[company_data_to_process]
 
 
DECLARE @Max_company_id BIGINT
       ,@Now [datetime2](6)
       
       
SET @Now = getutcdate()
 
SELECT  @Max_company_id=ISNULL(MAX(company_id),0)
FROM   [gold].[company]
 
PRINT @Max_company_id
PRINT @Now
 
 
 
-- End existing records
UPDATE [gold].[company]
SET
     [Valid_To]  = @Now
    ,[IsCurrent] = 0
FROM [gold].[company]
  INNER JOIN [gold].[company_data_to_process_data] AS cdp
ON [company].[company_bk] = cdp.[company_bk]
WHERE [company].[IsCurrent] = 1 -- ASSUMPTION: Only update current records
 
-- Insert new versions
INSERT INTO [gold].[company]
(
    [company_id],
    [company_bk],
    [ShortName],
    [LongName],
    [Symbol],
    [Address],
    [City],
    [Zip],
    [State],
    [Country],
    [Website],
    [Industry],
    [Sector],
    [Currency],
    [LogoURL],
    [StockMarket],
    [FetchDataTimestamp],
    [Fingerprint],
    [Valid_From],
    [Valid_To],
    [IsCurrent]
)
SELECT      ROW_NUMBER() OVER (ORDER BY [company_data_to_process_data].[company_bk] ASC) + @Max_company_id AS [company_id],
            [company_data_to_process_data].[company_bk],
            [company_data_to_process_data].[ShortName],
            [company_data_to_process_data].[LongName],
            [company_data_to_process_data].[Symbol],
            [company_data_to_process_data].[Address],
            [company_data_to_process_data].[City],
            [company_data_to_process_data].[Zip],
            [company_data_to_process_data].[State],
            [company_data_to_process_data].[Country],
            [company_data_to_process_data].[Website],
            [company_data_to_process_data].[Industry],
            [company_data_to_process_data].[Sector],
            [company_data_to_process_data].[Currency],
            [company_data_to_process_data].[LogoURL],
            [company_data_to_process_data].[StockMarket],
            [company_data_to_process_data].[FetchDataTimestamp],
            [company_data_to_process_data].[Fingerprint],
            CASE WHEN [company].[Valid_To] IS NOT NULL THEN @Now
                                                       ELSE CONVERT(DATETIME2, '2000-01-01')
                                                       END                                    AS [Valid_From],
            CONVERT(DATETIME2, '9999-12-31')                                                  AS [Valid_To],
            1                                                                                 AS [IsCurrent]
FROM [gold].[company_data_to_process_data]
  LEFT OUTER JOIN [gold].[company]
    ON [company_data_to_process_data].[company_bk] = [company].[company_bk]
    AND [company].[Valid_To] = @Now
 
 
-- Update company_current
TRUNCATE TABLE [gold].[company_current]
 
INSERT INTO  [gold].[company_current]
SELECT * FROM [gold].[company]
WHERE  [IsCurrent] = 1  
 
GO
 
--
 
EXEC [silver].[CreateVersionCompany]
EXEC [gold].[CreateSCD2Company]