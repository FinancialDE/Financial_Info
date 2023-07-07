# HHworking
HH's working code (Temporary)

## Local Environment Setup

- Put a `.env` file in this repository directory. An example content in `.env` would be:
```
YFINANCE_USER = ourYFinanceAccount@gmail.com
YFINANCE_PASSWORD = ourYFinancePW
AWS_RDS_HOST = ourHost
AWS_RDS_PORT= ourPORT
AWS_RDS_DB_NAME = ourNAME
AWS_RDS_DB_USER = user_name
AWS_RDS_DB_PASSWORD = ourPW
STOCK_API_KEY = 0URAPIKEYID
```
  

## Run Tests

- Run test for the `StockHistory_ETL` pipeline:
  `python -i tests/test_stock_history.py`

- Run test for the `BalanceSheet_ETL` pipeline:
  `python -i tests/test_balance_sheet.py`

- Run test for the `IncomeStatement_ETL` pipeline:
  `python -i tests/test_income_statement.py`

- Run test for the `CashFlow_ETL` pipeline:
  `python -i tests/test_cash_flow.py`

- Run test for the `CompanyInfo_ETL` pipeline:
  `python -i tests/test_company_info.py`

## Usage

- Demo notebooks can be viewed [here](./notebooks/demo/). 

## To Do's

- Add CIK to company_info ETL (requires interacting with FinancialInstitutions or the output table of FinancialInstitutions). 
- 

## Finished

- Transform data to new defined data schema. Check the [data directory](./data/) to view transformed data model.

- Change the transformed data output to parquet. Check the [data directory](./data/) to download the .parquet files.
  ``` python
  # To check the .parquet file

  import pandas as pd

  filename_parquet = './data/balance_sheet.parquet'
  df = pd.read_parquet(filename_parquet)
   
  ```