import os
import sys
sys.path.append('../')

from financial_etl.cash_flow import CashFlow_ETL

def test_extract(symbols, filename_out):
    
    etl = CashFlow_ETL()

    etl.extract(symbols, filename_out)

def test_transform(filename_in, filename_out):

    etl = CashFlow_ETL()

    df = etl.transform(filename_in, filename_out)

    return df


if __name__ == '__main__':

    dir_thisfile = os.path.dirname(os.path.abspath(__file__))
    dir_repo = os.path.join(dir_thisfile, '../')
    dir_data_lake = os.path.join(dir_repo, 'data_lake/')

    dir_data = os.path.join(dir_repo, 'data')

    if not os.path.exists(dir_data):
        os.makedirs(dir_data)

    symbols = ['JPM', 'GS']

    filename_raw = os.path.join(dir_data_lake, 'cash_flow.csv')
    #test_extract(symbols=symbols, filename_out=filename_raw)

    filename_out = os.path.join(dir_data, 'cash_flow.csv')
    df = test_transform(filename_in=filename_raw,filename_out=filename_out)