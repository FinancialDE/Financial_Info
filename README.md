# Financial_Info
ETL pipeline for financial institution information

## Local Environment Setup

- Put a `.env` file in this repository directory. An example content in `.env` would be:
```
YFINANCE_USER=email@gmail.com
YFINANCE_PASSWORD=pw
BUCKET_NAME='example_bucket'
OBJECT_KEY='example_key'
RAW_OBJECT_KEY='example_key'
REDSHIFT_ENDPOINT='host'
REDSHIFT_PORT='5439'
REDSHIFT_DB_NAME='dev'
REDSHIFT_USER='admin'
REDSHIFT_PASSWORD='pw'
PSYCOPG2= psycopg2.connect(
        dbname='dev',
        user='admin',
        password='pw',
        host='host',
        port='5439'
        )
ARN='example
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

