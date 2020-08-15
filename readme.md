# CFTC
Each Friday the CFTC publishes its Commitment of Traders reports showing long and short positions in financial and commodity futures as of that Tuesday.
This is the scraper for all the data published by CFTC, including:
* Disaggregated Futures Only Reports
* Disaggregated Futures-and-Options Combined Reports
* Traders in Financial Futures; Futures Only Reports
* Traders in Financial Futures; Futures-and-Options Combined Reports
* Futures Only Reports
* Futures-and-Options Combined Reports
* Commodity Index Trader Supplement


## config
PostgreSQL DB setting

## sql
the table create command for all the CFTC data

## data
if the table doesn't exist, this script will create for you
run the script each Friday after CFTC publishes the reports to update the data