--Create Schemas
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Core')
BEGIN
   EXEC('CREATE SCHEMA Core')
END
;

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Helper')
BEGIN
   EXEC('CREATE SCHEMA Helper')
END
;

--Create Tables
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Core.TransferLog') AND type in (N'U'))
BEGIN
   CREATE TABLE [Core].[TransferLog](
        [SourceObjectName] [varchar](128) NOT NULL,
        [StartTime] [datetime2](3) NOT NULL,
        [TransferLowWatermarkEH] [datetime2](3) NULL,
        [TransferHigwatermark] [datetime2](3) NOT NULL,
        [SuccessEndTime] [datetime2](3) NULL,
        [RowsInsertedEH] [int] NULL,
        [ExitErrorTime] [datetime2](3) NULL,
        [ErrorMessage] [varchar](max) NULL,
        [SparkMonitoringURL] [varchar](max) NULL,
        [PipelineRunId] [varchar](64) NOT NULL
   );
END
;

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Core.TransferObject') AND type in (N'U'))
BEGIN
   CREATE TABLE Core.TransferObject (
       SourceObjectName NVARCHAR(255),
       Batch INT,
       LoadMode NVARCHAR(50),
       Api NVARCHAR(255),
       Stockmarket NVARCHAR(50),
       LoadStatus NVARCHAR(50),
       WatermarkEH INT,
       Enabled BIT
   );
END
; 


--Create Stored Procedures
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Core].[StartTransfer]') AND type in (N'P', N'PC'))
BEGIN
    EXEC('CREATE PROCEDURE [Core].[StartTransfer]
              @SourceObjectName     VARCHAR (128),
              @HighWatermark        DATETIME2(3),
              @PipelineRunId        VARCHAR(64)
          AS
          BEGIN
              SET NOCOUNT ON;

              DECLARE @FormattedDate  VARCHAR(128),
                      @LakehousePath  VARCHAR(256),
                      @SourceFilePath VARCHAR(256),
                      @StartTime      DATETIME2(3);

              SET @StartTime = GETUTCDATE();

              BEGIN TRANSACTION;

              -- Update LoadStatus in TransferObject table
              UPDATE [Core].[TransferObject]
              SET LoadStatus = ''Running''
              WHERE SourceObjectName = @SourceObjectName;

              -- Insert a new record into TransferLog table
              INSERT INTO [Core].[TransferLog] (
                  SourceObjectName,
                  StartTime,
                  [TransferLowWatermarkEH],
                  TransferHigwatermark,
                  PipelineRunId        
              )
              SELECT
                  @SourceObjectName,
                  @StartTime,
                  WatermarkEH,
                  @HighWatermark,
                  @PipelineRunId   
              FROM [Core].[TransferObject]
              WHERE SourceObjectName = @SourceObjectName;

              COMMIT TRANSACTION;

              SELECT   @SourceObjectName                               AS SourceObjectName,
                       CONVERT(VARCHAR,       @HighWatermark, 127)     AS ModifyDateHighWaterMark,
                       CONVERT(VARCHAR,       [WatermarkEH],   127)    AS LowWaterMarkEH,
                       CONVERT(VARCHAR,       @HighWatermark, 127)     AS NewLowWatermarkEH,
                       LEFT(CONVERT(VARCHAR,  [WatermarkEH],  127),10) AS LowWaterMarkDateEH,
                       LEFT(CONVERT(VARCHAR,  @HighWatermark, 127),10) AS NewLowWatermarkDateEH,
                       @StartTime                                      AS StartTime,
                       [LoadMode],
                       [Api],
                       [Stockmarket]
              FROM [Core].[TransferObject]
              WHERE SourceObjectName = @SourceObjectName;
          END')
END
;

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Core].[GetLoadObjects]') AND type in (N'P', N'PC'))
BEGIN
    EXEC('CREATE PROCEDURE [Core].[GetLoadObjects]
          @Batch VARCHAR (25) = ''%''
          AS
          BEGIN
              /* Get list of SourceObjects (Stock Symbol) 
                 of all Stocks belonging to the selected Batch
                 and which are Enabled                          */
              SELECT SourceObjectName,
                     GETUTCDATE() AS HighWaterMark 
              FROM   [Core].[TransferObject]
              WHERE [Batch] LIKE @Batch
                AND [Enabled] = 1
          END')
END
;

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Core].[GetGetMetadataObjects]') AND type in (N'P', N'PC'))
BEGIN
    EXEC('CREATE PROCEDURE [Core].[GetGetMetadataObjects]
          @Batch VARCHAR (25) = ''%''
          AS
          BEGIN
              DECLARE @jsonResult NVARCHAR(MAX);

              SET @jsonResult = (
                  SELECT Stockmarket, STRING_AGG(SourceObjectName, '','') AS Symbol 
                  FROM   Core.TransferObject
                  WHERE [Batch] LIKE @Batch
                    AND [Enabled] = 1
                  GROUP BY Stockmarket
                  FOR JSON PATH);

              SELECT @jsonResult AS SymbolsToGetMetadata
          END')
END
;

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Core].[ErrorInTransfer]') AND type in (N'P', N'PC'))
BEGIN
    EXEC('CREATE PROCEDURE [Core].[ErrorInTransfer]
          @SourceObjectName VARCHAR(128),
          @StartTime DATETIME2(3),
          @ErrorMessage VARCHAR(MAX),
          @SparkMonitoringURL VARCHAR(MAX)
          AS
          BEGIN
              SET NOCOUNT ON;

              BEGIN TRANSACTION;

              -- Update LoadStatus in TransferObject table
              UPDATE [Core].[TransferObject]
              SET [LoadStatus] = ''Error End''
              WHERE SourceObjectName = @SourceObjectName;

              -- Update ErrorInfo in TransferLog table
              UPDATE [Core].[TransferLog]
              SET [ErrorMessage] = @ErrorMessage,
                  [SparkMonitoringURL] = @SparkMonitoringURL,
                  [ExitErrorTime] = GETUTCDATE()
              WHERE SourceObjectName = @SourceObjectName
                AND StartTime = @StartTime;

              COMMIT TRANSACTION;
              SELECT ''DONE, Make Pipeline Happy''
          END')
END
;

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Core].[EHDatabaseLoaded]') AND type in (N'P', N'PC'))
BEGIN
    EXEC('CREATE PROCEDURE [Core].[EHDatabaseLoaded]
          @SourceObjectName VARCHAR(128),
          @StartTime DATETIME2(3),
          @RowsInsertedKQL INT
          AS
          BEGIN
              SET NOCOUNT ON;

              BEGIN TRANSACTION;

              -- Update WatermarkKQL and LoadStatus in TransferObject table
              IF @RowsInsertedKQL > 0
              BEGIN
                  UPDATE [TransferObject]
                  SET [WatermarkEH] = (
                          SELECT TransferHigwatermark --TransferLowWatermarkKQL
                          FROM [Core].[TransferLog]
                          WHERE SourceObjectName = @SourceObjectName
                            AND StartTime = @StartTime
                      ),
                      [LoadStatus] = ''Successful loaded''
                  FROM  [Core].[TransferObject] AS [TransferObject]
                  WHERE [SourceObjectName] = @SourceObjectName;
              END
              ELSE  -- Only log the successful transfer, don''t change the watermark (different timezones)
              BEGIN
                  UPDATE [Core].[TransferObject]
                  SET    LoadStatus = ''Successful loaded''
                  WHERE SourceObjectName = @SourceObjectName;
              END

              -- Update RowsInsertedKQL in TransferLog table
              UPDATE [Core].[TransferLog]
              SET [RowsInsertedEH] = @RowsInsertedKQL,
                  [SuccessEndTime] = GETUTCDATE()
              WHERE [SourceObjectName] = @SourceObjectName
                AND [StartTime]        = @StartTime;

              COMMIT TRANSACTION;
              SELECT ''DONE, Make Pipeline Happy''
          END')
END
;

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Helper].[CleanUpAllData]') AND type in (N'P', N'PC'))
BEGIN
    EXEC('CREATE PROCEDURE [Helper].[CleanUpAllData]
              @DeleteData INT = 0
          AS
          BEGIN
              IF @DeleteData = 1
              BEGIN
                  DELETE FROM [Core].[TransferLog];
                  DELETE FROM [Core].[TransferObject];
                  PRINT ''Data deleted'';
              END
              ELSE
              BEGIN
                  PRINT ''No Data deleted, please use [Helper].[CleanUpAllData] @DeleteData=1 if you wish to delete data'';
              END
          END')
END
;

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[Helper].[LoadTransferObject]') AND type in (N'P', N'PC'))
BEGIN
    EXEC('CREATE PROCEDURE [Helper].[LoadTransferObject]
          AS
          BEGIN
              ;WITH TablesToTransferRaw AS (
                         SELECT ''MSFT''    AS SourceObjectName, NULL AS Batch, ''Daily'' AS LoadMode, ''Yahoo'' AS Api, NULL AS WatermarkEH, ''Dow Jones'' AS Stockmarket, 1 AS Enabled
              UNION ALL  SELECT ''GOOG''    AS SourceObjectName, NULL AS Batch, ''Daily'' AS LoadMode, ''Yahoo'' AS Api, NULL AS WatermarkEH, ''Dow Jones'' AS Stockmarket, 1 AS Enabled
              UNION ALL  SELECT ''ORCL''    AS SourceObjectName, NULL AS Batch, ''Daily'' AS LoadMode, ''Yahoo'' AS Api, NULL AS WatermarkEH, ''Dow Jones'' AS Stockmarket, 0 AS Enabled
              UNION ALL  SELECT ''NVDA''    AS SourceObjectName, NULL AS Batch, ''Daily'' AS LoadMode, ''Yahoo'' AS Api, NULL AS WatermarkEH, ''Dow Jones'' AS Stockmarket, 1 AS Enabled
              UNION ALL  SELECT ''AAPL''    AS SourceObjectName, NULL AS Batch, ''Daily'' AS LoadMode, ''Yahoo'' AS Api, NULL AS WatermarkEH, ''Dow Jones'' AS Stockmarket, 1 AS Enabled
              UNION ALL  SELECT ''NESN.SW'' AS SourceObjectName, NULL AS Batch, ''Daily'' AS LoadMode, ''Yahoo'' AS Api, NULL AS WatermarkEH, ''SMI''       AS Stockmarket, 1 AS Enabled
              UNION ALL  SELECT ''NOVN.SW'' AS SourceObjectName, NULL AS Batch, ''Daily'' AS LoadMode, ''Yahoo'' AS Api, NULL AS WatermarkEH, ''SMI''       AS Stockmarket, 1 AS Enabled
              UNION ALL  SELECT ''ROG.SW''  AS SourceObjectName, NULL AS Batch, ''Daily'' AS LoadMode, ''Yahoo'' AS Api, NULL AS WatermarkEH, ''SMI''       AS Stockmarket, 0 AS Enabled
              UNION ALL  SELECT ''ABBN.SW'' AS SourceObjectName, NULL AS Batch, ''Daily'' AS LoadMode, ''Yahoo'' AS Api, NULL AS WatermarkEH, ''SMI''       AS Stockmarket, 1 AS Enabled
              UNION ALL  SELECT ''UBSG.SW'' AS SourceObjectName, NULL AS Batch, ''Daily'' AS LoadMode, ''Yahoo'' AS Api, NULL AS WatermarkEH, ''SMI''       AS Stockmarket, 1 AS Enabled
              UNION ALL  SELECT ''SREN.SW'' AS SourceObjectName, NULL AS Batch, ''Daily'' AS LoadMode, ''Yahoo'' AS Api, NULL AS WatermarkEH, ''SMI''       AS Stockmarket, 1 AS Enabled

              )
              , TablesToTransfer
              AS
              (
              SELECT SourceObjectName,
                     COALESCE(Batch, ''GeneralProcessing'') AS Batch,
                     COALESCE(LoadMode, ''Daily'') AS LoadMode,
                     Api,
                     ''New Definition'' AS LoadStatus,
                     --COALESCE(WatermarkEH, DATEADD(DAY, -20, GETUTCDATE())) AS WatermarkEH,
                     COALESCE(WatermarkEH, CONVERT(DATETIME, ''19000101'', 112)) AS WatermarkEH,
                     Stockmarket,
                    Enabled
              FROM TablesToTransferRaw
              )
              -- Use the CTE in the MERGE statement
              MERGE [Core].[TransferObject] AS target
              USING TablesToTransfer AS source
              ON target.SourceObjectName = source.SourceObjectName
              WHEN MATCHED THEN
                  UPDATE SET       
                      target.[SourceObjectName]   = source.[SourceObjectName],
                      target.[Batch]               = source.[Batch],
                      target.[LoadMode]            = source.[LoadMode],
                      target.[Api]                 = source.[Api],
                      target.[LoadStatus]          = source.[LoadStatus],
                      target.[WatermarkEH]         = source.[WatermarkEH],
                      target.[Stockmarket]         = source.[Stockmarket],
                      target.[Enabled]             = source.[Enabled]
                      
              WHEN NOT MATCHED THEN
                  INSERT (
                     [SourceObjectName],
                     [Batch],
                     [LoadMode],
                     [Api],
                     [LoadStatus],
                     [WatermarkEH],
                     [Stockmarket],
                     [Enabled]
              )
                  VALUES 
                  (      
                     source.[SourceObjectName],
                     source.[Batch],
                     source.[LoadMode],
                     source.[Api],
                     source.[LoadStatus],
                     source.[WatermarkEH],
                     source.[Stockmarket],
                     source.[Enabled]
              );
          END')
END
;