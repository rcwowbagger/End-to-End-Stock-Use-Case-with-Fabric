In this section we add some bonus material to enhance the solution.

### Bonus 1

The company function to enable SCD in the eventhouse has an error which will add everytime the Company Pipeline runs a new row for any change (in this case Microsoft as one change has been enforced on the currency column). The screenshot shows the Silver_Company table after a third run. Can you find the issue and solve it?

<img src="../PNG/Bonus%201%20Silver%20Company%20Table%20multiplication.png" width="500">

The body of the function which needs to be adjusted looks like following:

```ruby
let companyCount=toscalar(Silver_Company | count);
external_table('bronze_company')
| extend companyCount
| extend Currency = iff(Symbol == 'MSFT' and companyCount == 0, 'XXX', Currency)  // To simulate update
| join kind=leftouter Silver_Company on Symbol
| extend BronzeHash = hash(strcat(ShortName, LongName, Address, City, Zip, State, Country, Website, Industry, Sector, Currency, LogoURL, StockMarket))
| where  BronzeHash <> Hash
| project ShortName, LongName, Symbol, Address, City, Zip, State, Country, Website, Industry, Sector, Currency, LogoURL, FetchDataTimestamp, StockMarket, Hash = BronzeHash
```

Find the solution [here](Solution%20Bonus%201.md).

### Bonus 2

Instead of doing SCD in the Eventhouse, we can leverage the new ["Accelerate Shortcut"](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/query-acceleration) feature and keep track of any change of our dimensions in a Warehouse. To do so, please follow these instructions:

1. Create a new Warehouse in your Fabric Workspace
2. Run [this SQL script](../SQL%20Script/Bonus%202%20Create%20SCD%20in%20Warehouse.sql) to create the required schemas, tables, and stored procedures.
3. Adjust the Get Company pipeline to call the two new stored procedures
4. Create two accelerated shortcuts in your Eventhouse to the two new gold tables from your new Warehouse
5. Run the pipeline and check if your data is available in the Eventhouse
6. Update your Power BI report to point to new company table. Most likely, you'll not be interested in the SCD of data, so just connect to the current company table.

<img src="../PNG/Bonus%204%20Update%20Pipeline.png" width="500">
<img src="../PNG/Bonus%202%20Create%20Shortcut%20on%20WH%20tables.png" width="500">
<img src="../PNG/Bonus%203%20Enable%20Accelerated%20Shortcuts.png" width="500">
