Left join on Company table (Gold) instead of silver_company where  BronzeHash <> Hash should only be tested against the latest record in the company table

```
let companyCount=toscalar(Silver_Company | count);
external_table('bronze_company')
| extend companyCount
| extend Currency = iff(Symbol == 'MSFT' and companyCount == 0, 'XXX', Currency)  // To simulate update
| join kind=leftouter Company on Symbol
| extend BronzeHash = hash(strcat(ShortName, LongName, Address, City, Zip, State, Country, Website, Industry, Sector, Currency, LogoURL, StockMarket))
| where  BronzeHash <> Hash
| project ShortName, LongName, Symbol, Address, City, Zip, State, Country, Website, Industry, Sector, Currency, LogoURL, FetchDataTimestamp, StockMarket, Hash = BronzeHash
```