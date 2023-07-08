import os
from yahooquery import Ticker
from financial_etl.base import Base_ETL
import pandas as pd
import boto3

class BalanceSheet_ETL(Base_ETL):

    def __init__(self):
        super().__init__()
        self._username = os.getenv('YFINANCE_USER')
        self._password = os.getenv('YFINANCE_PASSWORD')

        self.columns_to_keep = ['symbol', 'date', 'TotalAssets', 'TotalLiabilitiesNetMinorityInterest',
                                'TotalEquityGrossMinorityInterest', 'TotalCapitalization', 'PreferredStockEquity',
                                'CommonStockEquity', 'NetTangibleAssets', 'InvestedCapital', 'TangibleBookValue',
                                'TotalDebt', 'NetDebt', 'ShareIssued', 'OrdinarySharesNumber',
                                'PreferredSharesNumber', 'TreasurySharesNumber']

    def extract(self, symbols, filename_out=None):
        '''Extract Balance Sheet with Yfianace API

            Args: 
                symbols = ['JPM', 'GS', 'MS', 'SIVBQ']
            
            Returns:
                pd.DataFrame
        '''

        raw_data = Ticker(symbols=symbols, username=self._username, password=self._password).p_balance_sheet(trailing=False, frequency='q')

        if filename_out is None:
            if not os.path.exists(self.dir_data_lake):
                os.makedirs(self.dir_data_lake)
            
            filename_out = os.path.join(self.dir_data_lake, 'balance_sheet.csv')

        bucket_name = 'lg18dagbucket'
        object_key = 's3://lg18dagbucket/STAGING/balance_sheet.csv'

        s3_client = boto3.client('s3')
        s3_client.upload_file(filename_out, bucket_name, object_key)
        
    
    def _rename_columns(self, df, column_rename_mapping={'asOfDate': 'date'}):
        return df.rename(columns=column_rename_mapping)
    
    def _filter_by_date_range(self, df, date_range):
        df['date'] = pd.to_datetime(df['date'])
        df = df.loc[df['date'].isin(pd.date_range(start=date_range[0], end=date_range[1]))]
        return df.reset_index(drop=True)
    
    def transform(self, filename_in, filename_out, save_mode='parquet'):

        df_raw = pd.read_csv(filename_in)

        df = self._rename_columns(df_raw, column_rename_mapping={'asOfDate': 'date'})

        df = self._filter_by_date_range(df, date_range=['2017-01-01', '2022-03-31'])
        
        df = df[self.columns_to_keep]

        if save_mode == 'parquet':
            bucket_name = 'lg18dagbucket'
            object_key = 's3://lg18dagbucket/to_warehouse/balance_sheet.csv'

            s3_client = boto3.client('s3')
            s3_client.upload_file(filename_out, bucket_name, object_key)
        
        elif save_mode == 'csv':
            bucket_name = 'lg18dagbucket'
            object_key = 's3://lg18dagbucket/to_warehouse/balance_sheet_cleaned.csv'

            s3_client = boto3.client('s3')
            s3_client.upload_file(filename_out, bucket_name, object_key)
        

        return df
