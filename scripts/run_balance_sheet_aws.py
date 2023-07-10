import os
import sys
sys.path.append('../')

from financial_etl.balance_sheet import BalanceSheet_ETL
class DataProcessor:
    def __init__(self):
        self.symbols = ['JPM', 'GS', 'MS', 'BA', 'SIVBQ']
        self.dir_thisfile = os.path.dirname(os.path.abspath(__file__))
        self.dir_repo = os.path.join(self.dir_thisfile, '../')
        self.dir_data_lake = os.path.join(self.dir_repo, 'data_lake/')
        self.dir_data = os.path.join(self.dir_repo, 'data')

        if not os.path.exists(self.dir_data):
            os.makedirs(self.dir_data)

        self.filename_raw = os.path.join(self.dir_data_lake, 'balance_sheet.csv')
        self.filename_out = os.path.join(self.dir_data, 'balance_sheet.csv')

    def extract_data(self):
        etl = BalanceSheet_ETL()
        etl.extract(self.symbols, self.filename_raw)

    def transform_data(self):
        etl = BalanceSheet_ETL()
        return etl.transform(self.filename_raw, self.filename_out, save_mode='csv')

    def load_data(self):
        etl = BalanceSheet_ETL()
        etl.load()

        
    def clean_ec2(self):
        etl = BalanceSheet_ETL()
        etl.remove_files()
# Usage:
if __name__ == '__main__':
    processor = DataProcessor()
    processor.extract_data()
    processor.transform_data()
    processor.load_data()
    processor.clean_ec2()

