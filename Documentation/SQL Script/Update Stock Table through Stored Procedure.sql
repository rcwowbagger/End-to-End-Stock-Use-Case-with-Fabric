ALTER PROCEDURE [Helper].[LoadTransferObject]
          AS
          BEGIN
              ;WITH TablesToTransferRaw AS (
                         SELECT 'MSFT'    AS SourceObjectName, NULL AS Batch, 'Daily' AS LoadMode, 'YahooFinance' AS Api, NULL AS WatermarkEH, 'Dow Jones' AS Stockmarket, 1 AS Enabled
              UNION ALL  SELECT 'GOOG'    AS SourceObjectName, NULL AS Batch, 'Daily' AS LoadMode, 'YahooFinance' AS Api, NULL AS WatermarkEH, 'Dow Jones' AS Stockmarket, 1 AS Enabled
              UNION ALL  SELECT 'ORCL'    AS SourceObjectName, NULL AS Batch, 'Daily' AS LoadMode, 'YahooFinance' AS Api, NULL AS WatermarkEH, 'Dow Jones' AS Stockmarket, 0 AS Enabled
              UNION ALL  SELECT 'NVDA'    AS SourceObjectName, NULL AS Batch, 'Daily' AS LoadMode, 'YahooFinance' AS Api, NULL AS WatermarkEH, 'Dow Jones' AS Stockmarket, 1 AS Enabled
              UNION ALL  SELECT 'AAPL'    AS SourceObjectName, NULL AS Batch, 'Daily' AS LoadMode, 'YahooFinance' AS Api, NULL AS WatermarkEH, 'Dow Jones' AS Stockmarket, 1 AS Enabled
              UNION ALL  SELECT 'NESN.SW' AS SourceObjectName, NULL AS Batch, 'Daily' AS LoadMode, 'YahooFinance' AS Api, NULL AS WatermarkEH, 'SMI'       AS Stockmarket, 0 AS Enabled
              UNION ALL  SELECT 'NOVN.SW' AS SourceObjectName, NULL AS Batch, 'Daily' AS LoadMode, 'YahooFinance' AS Api, NULL AS WatermarkEH, 'SMI'       AS Stockmarket, 0 AS Enabled
              UNION ALL  SELECT 'ROG.SW'  AS SourceObjectName, NULL AS Batch, 'Daily' AS LoadMode, 'YahooFinance' AS Api, NULL AS WatermarkEH, 'SMI'       AS Stockmarket, 0 AS Enabled
              UNION ALL  SELECT 'ABBN.SW' AS SourceObjectName, NULL AS Batch, 'Daily' AS LoadMode, 'YahooFinance' AS Api, NULL AS WatermarkEH, 'SMI'       AS Stockmarket, 0 AS Enabled
              UNION ALL  SELECT 'UBSG.SW' AS SourceObjectName, NULL AS Batch, 'Daily' AS LoadMode, 'YahooFinance' AS Api, NULL AS WatermarkEH, 'SMI'       AS Stockmarket, 1 AS Enabled
              UNION ALL  SELECT 'SREN.SW' AS SourceObjectName, NULL AS Batch, 'Daily' AS LoadMode, 'YahooFinance' AS Api, NULL AS WatermarkEH, 'SMI'       AS Stockmarket, 0 AS Enabled

              )
              , TablesToTransfer
              AS
              (
              SELECT SourceObjectName,
                     COALESCE(Batch, 'GeneralProcessing') AS Batch,
                     COALESCE(LoadMode, 'Daily') AS LoadMode,
                     Api,
                     'New Definition' AS LoadStatus,
                     --COALESCE(WatermarkEH, DATEADD(DAY, -20, GETUTCDATE())) AS WatermarkEH,
                     COALESCE(WatermarkEH, CONVERT(DATETIME, '20200101', 112)) AS WatermarkEH,
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
          END
