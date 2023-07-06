import os
import sys
sys.path.append('../')

from financial_etl.stock_history import StockHistory_ETL


def test_extract(symbols, dir_data_lake):

    etl = StockHistory_ETL()


    for symbol in symbols:

        filename = os.path.join(dir_data_lake, symbol+'.json')
        etl.extract(symbol, filename)


def test_transform(symbols, dir_data_lake, filename_out):

    etl = StockHistory_ETL()

    df = etl.transform(symbols, dir_data_lake, filename_out)

    return df


if __name__ == '__main__':

    dir_thisfile = os.path.dirname(os.path.abspath(__file__))
    dir_parent = os.path.join(dir_thisfile, '../')
    dir_data_lake = os.path.join(dir_parent, 'data_lake/stock_history/')

    dir_data = os.path.join(dir_parent, 'data')

    if not os.path.exists(dir_data):
        os.makedirs(dir_data)

    symbols = ['JPM', 'GS']


    test_extract(symbols, dir_data_lake)

    filename = os.path.join(dir_data, 'stock_history.csv')
    df = test_transform(symbols, dir_data_lake, filename)
