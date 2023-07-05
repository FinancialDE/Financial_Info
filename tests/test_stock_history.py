import os
import sys
sys.path.append('../')

from financial_etl.stock_history import StockHistory_ETL


def test_extract(symbols, dir_data):

    etl = StockHistory_ETL()


    for symbol in symbols:

        filename = os.path.join(dir_data, symbol+'.json')
        etl.extract(symbol, filename)


def test_transform(symbols, file_csv):

    etl = StockHistory_ETL()

    dir_thisfile = os.path.dirname(os.path.abspath(__file__))
    dir_parent = os.path.join(dir_thisfile, '../')
    dir_data = os.path.join(dir_parent, 'data_lake/stock_history/')

    df_list = []
    for symbol in symbols:
        
        df = etl.transform(symbol, dir_data)
        df_list.append(df)
    
    joined_df = etl.concat_dataframes(df_list)
    etl.save_to_csv(joined_df, filename=file_csv)

    return joined_df


if __name__ == '__main__':

    dir_thisfile = os.path.dirname(os.path.abspath(__file__))
    dir_parent = os.path.join(dir_thisfile, '../')
    dir_data_lake = os.path.join(dir_parent, 'data_lake/stock_history/')

    dir_data = os.path.join(dir_parent, 'data')

    if not os.path.exists(dir_data):
        os.makedirs(dir_data)

    symbols = ['JPM', 'GS']


    test_extract(symbols, dir_data=dir_data_lake)

    file_csv = os.path.join(dir_data, 'stock_history.csv')
    df = test_transform(symbols, file_csv)
