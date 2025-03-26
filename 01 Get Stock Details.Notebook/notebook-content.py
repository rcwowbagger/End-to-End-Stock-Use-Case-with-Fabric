# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4f700ffd-2c1f-476a-8bc0-a7d8a739e4fa",
# META       "default_lakehouse_name": "LH_Stocks",
# META       "default_lakehouse_workspace_id": "9356cf30-1611-4a5c-94b0-f2af55effd60",
# META       "known_lakehouses": []
# META     },
# META     "environment": {
# META       "environmentId": "55ea9b9f-aee3-88a3-4417-95fa1b732c64",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Import required Libraries

# CELL ********************

#Import needed libraries
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, date
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, LongType, TimestampType
from pyspark.sql.functions import col, min, max
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Define global parameters

# PARAMETERS CELL ********************

#dynamic parameters
pipelineRunId = 'manual'

api = 'YahooFinance'
stockmarket = 'dowJones'
stock = 'MSFT'

startDate = '2024-11-14'
endDate = '2024-11-15'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Define fixed configuration paramters
# 
# kusto_cluster = "https://xxx.z1.kusto.fabric.microsoft.com" <br>
# kusto_database = "EH_Stocks" <br>
# kusto_table = "Bronze_Stock" <br><br>
# columnNames = ["date", "open", "high", "low", "close", "volume", "dividends", "stockSplits"]

# CELL ********************

#Define fixed configuration Parameters

#Kusto parameters
kustoCluster = "https://trd-tshp09829tw6dtfu7t.z1.kusto.fabric.microsoft.com"
kustoDatabase = "EH_Stocks"
kustoTable = "Bronze_Stock"

#Define column names for Dataframe holding all stocks
columnNames = ["date", "open", "high", "low", "close", "volume", "dividends", "stockSplits"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

#prepare parameters
fetchDataTimestamp = datetime.now()

startDate = datetime.strptime(startDate, '%Y-%m-%d').date()
endDate = datetime.strptime(endDate, '%Y-%m-%d').date()

startYear = startDate.year
endYear = endDate.year
stockDetailYearsList = list(range(startYear, endYear + 1))

#Define integer to count how many rows have been created
countRows = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not startDate == endDate:

    stockDetails = yf.Ticker(stock)

    for year in stockDetailYearsList:
        if startYear == year:
            startDate_current_running_year = startDate
        else:
            startDate_current_running_year = datetime(year, 1, 1).date()
            
        if endYear == year:
            endDate_current_running_year = endDate
        else:
            endDate_current_running_year = datetime(year, 12, 31).date()

        print("Current run:", startDate, "/ startDate:", startDate_current_running_year, "/ endDate:", endDate_current_running_year)

        #Get history
        df_stock_details = stockDetails.history(start=startDate_current_running_year, end=endDate_current_running_year)

        if df_stock_details.empty:
            continue
            print("skipped")
        else:
            df_stock_details.index = df_stock_details.index.date
            df_stock_details.reset_index(inplace=True)

            df_stock_details.columns = columnNames

            stock_details_string = df_stock_details.to_json(orient="records")
            stock_details_json = json.loads(stock_details_string)

            #Prepare JSON for Kusto
            json_structure = {
                "main": {
                    "api": api,
                    "stockmarket": stockmarket,
                    "symbol": stock,
                    "fetchDataTimestamp": fetchDataTimestamp.timestamp(),
                    "pipelineRunId": pipelineRunId
                },
                "data": stock_details_json
            }

            json_string = json.dumps(json_structure)

            json_data = {'StockInfo': [json_string]}

            #Convert data into a Dataframe
            df_stock_details_json = pd.DataFrame(json_data)
            df_stock_details_json_spark = spark.createDataFrame(df_stock_details_json)
            
            #Get Access Token for Kusto
            accessToken = notebookutils.credentials.getToken('kusto')

            # Write data to stock table in Eventhouse
            df_stock_details_json_spark.write. \
            format("com.microsoft.kusto.spark.synapse.datasource"). \
            option("kustoCluster",kustoCluster). \
            option("kustoDatabase",kustoDatabase). \
            option("kustoTable", kustoTable). \
            option("accessToken", accessToken). \
            option("tableCreateOptions", "CreateIfNotExist"). \
            mode("Append"). \
            save()
            #option("writeMode", "Queued"). \

            #Create an exit value for our log files. We're counting how many rows have been created.
            countRowsCurrentRun = df_stock_details['date'].count()
            countRows = countRows + countRowsCurrentRun

mssparkutils.notebook.exit(countRows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
