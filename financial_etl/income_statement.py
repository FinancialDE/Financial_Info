import os
from yahooquery import Ticker
from financial_etl.base import Base_ETL
import pandas as pd

class IncomeStatement_ETL(Base_ETL):
    
    def __init__(self):
        self._username = os.getenv('YFINANCE_USER')
        self._password = os.getenv('YFINANCE_PASSWORD')

        self.dir_thisfile = os.path.dirname(os.path.abspath(__file__))
        self.dir_repo = os.path.join(self.dir_thisfile, '../')
        self.dir_data_lake = os.path.join(self.dir_repo, 'data_lake')
        self.dir_data = os.path.join(self.dir_repo, 'data')

    def extract(self, symbols, filename_out=None):
        '''Extract Income Statement with Yfianace API

            Args: 
                symbols = ['JPM', 'GS', 'MS', 'SIVBQ']
            
            Returns:
                pd.DataFrame
        '''

        raw_data = Ticker(symbols=symbols, username=self._username, password=self._password).p_income_statement(trailing=False, frequency='q')

        if filename_out is None:
            if not os.path.exists(self.dir_data_lake):
                os.makedirs(self.dir_data_lake)
            
            filename_out = os.path.join(self.dir_data_lake, 'income_statement.csv')

        raw_data.to_csv(filename_out, index=True)
    
    def _filter_columns(self, df, drop_threshold=0.8):
        ''' Drop columns that have a high percentage of null values'''

        null_fractions = df.isnull().mean()
        columns_to_drop = null_fractions[null_fractions > drop_threshold].index
        print('Columns to drop:', columns_to_drop)
        
        return df.drop(columns=columns_to_drop)
    
    def _rename_columns(self, df, column_rename_mapping={'asOfDate': 'date'}):
        return df.rename(columns=column_rename_mapping)
    
    def _filter_by_date_range(self, df, date_range):
        df['date'] = pd.to_datetime(df['date'])
        df = df.loc[df['date'].isin(pd.date_range(start=date_range[0], end=date_range[1]))]
        return df.reset_index(drop=True)
    
    def transform(self, filename_in, filename_out, drop_threshold=0.8):

        df_raw = pd.read_csv(filename_in)

        df = self._rename_columns(df_raw, column_rename_mapping={'asOfDate': 'date'})
        
        df = self._filter_by_date_range(df, date_range=['2017-01-01', '2022-03-31'])

        df = self._filter_columns(df, drop_threshold)

        df.to_csv(filename_out, index=False)

        return df
