import os
import sys
sys.path.append('../')

from financial_etl.financial_institutions import FinancialInstitutions

from financial_etl.stock_history import StockHistory_ETL
from financial_etl.cash_flow import CashFlow_ETL
from financial_etl.balance_sheet import BalanceSheet_ETL
from financial_etl.income_statement import IncomeStatement_ETL


if __name__ == '__main__':

    dir_thisfile = os.path.dirname(os.path.abspath(__file__))
    dir_repo = os.path.join(dir_thisfile, '../../')
    sec_server = FinancialInstitutions()
    sec_server.fetch_data(False)
    print(sec_server.count())
    symbols = sec_server.tickers()

    # expect symbols to include SVB and JPM
    desired_symbols = ['JPM', 'GS', 'SVB']

    # get the intersection of the two lists
    symbols = list(set(symbols) & set(desired_symbols))
    print('yes, these symbols are present in the list: ', symbols)
