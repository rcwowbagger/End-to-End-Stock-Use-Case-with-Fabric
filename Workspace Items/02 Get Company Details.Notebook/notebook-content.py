# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "484cd190-235a-42e8-9c57-59fa96268697",
# META       "default_lakehouse_name": "LH_Stocks",
# META       "default_lakehouse_workspace_id": "6fe94400-e137-4315-a0bc-8f8d615546cc",
# META       "known_lakehouses": [
# META         {
# META           "id": "484cd190-235a-42e8-9c57-59fa96268697"
# META         }
# META       ]
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
import yahoo_fin.stock_info as si
import pandas as pd
from datetime import datetime, timedelta, date
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Define global parameters

# PARAMETERS CELL ********************

stocks_string = '[{"Stockmarket":"dowJones","Symbol":"MSFT"},{"Stockmarket":"SMI","Symbol":"UBSG.SW"}]'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Define internal parameters

# CELL ********************

#Define Parameters
stocks_json = json.loads(stocks_string)
fetchDataTimestamp = datetime.now()

#Define intersted indexes
"""
dow = si.tickers_dow()
sp500 = si.tickers_sp500()
smi = ['NESN.SW', 'NOVN.SW', 'ROG.SW', 'ABBN.SW', 'CFR.SW', 'UBSG.SW', 'LONN.SW', 'SIKA.SW', 'GIVN.SW', 'ALC.SW', '0QKY.IL', 'CSGN.SW', 'SREN.SW', 'PGHN.SW', 'GEBN.SW', 'SGSN.SW', 'SLHN.SW', 'SCMN.SW', 'LOGN.SW', 'ZURN.SW']
dax = ['ADS.DE', 'AIR.DE', 'ALV.DE', 'BAS.DE', 'BAYN.DE', 'BEI.DE', 'BMW.DE', 'BNR.DE', 'CON.DE', '1COV.DE', 'DHER.DE', 'DBK.DE', 'DPW.DE', 'DTE.DE', 'EOAN.DE', 'FRE.DE', 'FME.DE', 'HEI.DE', 'HFG.DE', 'HEN3.DE', 'IFX.DE', 'LIN.DE', 'MBG.DE', 'MRK.DE', 'MTX.DE', 'MUV2.DE', 'PAH3.DE', 'PUM.DE', 'QIA.DE', 'RWE.DE', 'SAP.DE', 'SRT3.DE', 'SIE.DE', 'ENR.DE', 'SHL.DE', 'SY1.DE', 'VOW3.DE', 'VNA.DE', 'ZAL.DE']
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Define function to get company details

# CELL ********************

#Define function to get company details for provided stock
def get_company_details(stockmarket, stock):
        current_stock = yf.Ticker(stock)
        info = current_stock.info

        df = pd.DataFrame(list(info.items()), columns= ["Info", "Details"])    
        df = df.T
        new_header = df.iloc[0]
        df = df[1:]
        df.columns = new_header

        if not set(["shortName"]).issubset(df.columns):
            df['shortName'] = ""
        
        if not set(["longName"]).issubset(df.columns):
            df['longName'] = ""
        
        if not set(["symbol"]).issubset(df.columns):
            df['symbol'] = ""
        
        if not set(["address1"]).issubset(df.columns):
            df['address1'] = ""
        
        if not set(["city"]).issubset(df.columns):
            df['city'] = ""
        
        if not set(["zip"]).issubset(df.columns):
            df['zip'] = ""
        
        if not set(["state"]).issubset(df.columns):
            df['state'] = ""

        if not set(["country"]).issubset(df.columns):
            df['country'] = ""

        if not set(["website"]).issubset(df.columns):
            df['website'] = ""

        if not set(["industry"]).issubset(df.columns):
            df['industry'] = ""

        if not set(["sector"]).issubset(df.columns):
            df['sector'] = ""

        if not set(["currency"]).issubset(df.columns):
            df['currency'] = ""

        if not set(["logo_url"]).issubset(df.columns):
            df['logo_url'] = ""

        df = df[["shortName", "longName", "symbol", "address1", "city", "zip", "state", "country", "website", "industry", "sector", "currency", "logo_url"]]
        df['fetchDataTimestamp'] = fetchDataTimestamp
        df['stockmarket'] = stockmarket
        df['symbol'] = stock

        return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Get company details for each stock provided

# CELL ********************

#Define column names for Dataframe holding all stocks
column_names = ["shortName", "longName", "symbol", "address1", "city", "zip", "state", "country", "website", "industry", "sector", "currency", "logo_url"]
df_all_companies = pd.DataFrame(columns=column_names)

#Get company details for each stock by calling the function

for item in stocks_json:
    stockmarket = item["Stockmarket"]
    symbols = item["Symbol"].split(',')
    for symbol in symbols:
        df_current_company = get_company_details(stockmarket, symbol)
        df_all_companies = pd.concat([df_all_companies, df_current_company], ignore_index=True)

df_all_companies = df_all_companies.rename(columns={'shortName':'ShortName', 'longName':'LongName', 'symbol':'Symbol', 'address1':'Address', 'city':'City', 'zip':'Zip', 'state':'State', 'country':'Country', 'website':'Website', 'industry':'Industry', 'sector':'Sector', 'currency':'Currency', 'logo_url':'LogoURL', 'fetchDataTimestamp':'FetchDataTimestamp', 'stockmarket':'StockMarket'})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_all_companies)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Save result into Lakehouse

# CELL ********************

#Convert Pandas DataFrame to PySpark DataFrame
pyspark_df_all_companies = spark.createDataFrame(df_all_companies)

#Save dataframe as table into Lakehouse
pyspark_df_all_companies.write.mode("overwrite").saveAsTable('Bronze_Company')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
