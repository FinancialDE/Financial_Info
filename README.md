# HHworking
HH's working code (Temporary)


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

## To Do's

- Re-define data schema
- transfrom data output from csv storage to parquet.
- Add CIK to company_info ETL (requires interacting with FinancialInstitutions or the output table of FinancialInstitutions). 

