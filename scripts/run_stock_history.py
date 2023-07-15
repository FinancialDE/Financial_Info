import os
import sys

sys.path.append('../')

from financial_etl.stock_history import StockHistory_ETL


class DataProcessor:
    def __init__(self):
        self.symbols = ['JPM', 'GS', 'MS', 'BA', 'SIVBQ']
        self.dir_thisfile = os.path.dirname(os.path.abspath(__file__))
        self.dir_repo = os.path.join(self.dir_thisfile, '../')
        self.dir_data_lake = os.path.join(self.dir_repo, 'data_lake/stock_history/')
        self.dir_data = os.path.join(self.dir_repo, 'data')

        if not os.path.exists(self.dir_data):
            os.makedirs(self.dir_data)


    def extract_data(self):
        etl = StockHistory_ETL()

        for symbol in self.symbols:

            filename = os.path.join(self.dir_data_lake, symbol+'.json')
            etl.extract(symbol, filename)
    def transform_data(self):
        etl = StockHistory_ETL()

        etl.transform(self.symbols, self.dir_data_lake, save_mode='csv')

    def load_data(self):
        etl = StockHistory_ETL()
        etl.load()

    def clean_ec2(self):
        etl = StockHistory_ETL()
        etl.remove_files()


# Usage:
if __name__ == '__main__':
    processor = DataProcessor()
    processor.extract_data()
    processor.transform_data()
    processor.load_data()
#    processor.clean_ec2()
#

