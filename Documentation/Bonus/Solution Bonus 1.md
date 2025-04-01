As we're doing a left join on the Silver_Company table, we'll always have a difference as we're not comparing to the newest record from our gold table. Therefore, we need to adjust the left join to our Company table sitting in our gold layer to avoid the error.

```ruby
let companyCount=toscalar(Silver_Company | count);
external_table('bronze_company')
| extend companyCount
| extend Currency = iff(Symbol == 'MSFT' and companyCount == 0, 'XXX', Currency)  // To simulate update
| join kind=leftouter Company on Symbol
| extend BronzeHash = hash(strcat(ShortName, LongName, Address, City, Zip, State, Country, Website, Industry, Sector, Currency, LogoURL, StockMarket))
| where  BronzeHash <> Hash
| project ShortName, LongName, Symbol, Address, City, Zip, State, Country, Website, Industry, Sector, Currency, LogoURL, FetchDataTimestamp, StockMarket, Hash = BronzeHash
```