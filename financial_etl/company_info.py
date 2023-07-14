import os
import json
from yahooquery import Ticker
from financial_etl.base import Base_ETL
import pandas as pd

class CompanyInfo_ETL(Base_ETL):

    def __init__(self):
        super().__init__()
        self._username = os.getenv('YFINANCE_USER')
        self._password = os.getenv('YFINANCE_PASSWORD')

    def extract(self, symbols, filename_out=None):
        '''Extract Company Info with Yfianace API

            Args:
                symbols = ['JPM', 'GS', 'MS', 'SIVBQ']

            Returns:
                pd.DataFrame
        '''

        raw_data = Ticker(symbols=symbols, username=self._username, password=self._password).asset_profile

        if filename_out is None:
            if not os.path.exists(self.dir_data_lake):
                os.makedirs(self.dir_data_lake)

            filename_out = os.path.join(self.dir_data_lake, 'company_info.json')
        self.save_to_json(raw_data, filename_out)
        object_key = 's3://lg18dagbucket/STAGING/company_info.json'
        self._save_data_in_s3(filename_out, object_key)

    def _rename_columns(self, df, column_rename_mapping={'asOfDate': 'date'}):
        return df.rename(columns=column_rename_mapping)

    def transform(self, filename_in, filename_out, save_mode='parquet'):
        with open(filename_in, "r") as file:
            raw_data = json.load(file)

        # ------ Select only necessary columns ------
        columns_to_keep=['address1', 'city', 'state', 'zip', 'country', 'phone', 'website', 'industry', 'sector', 'longBusinessSummary', 'fullTimeEmployees']

        dict_list = []
        for symbol in raw_data.keys():

            selected_company_info = {key: raw_data[symbol][key] if key in raw_data[symbol] else None for key in columns_to_keep}

            selected_company_info['symbol'] = symbol

            dict_list.append(selected_company_info)

        df = pd.DataFrame(dict_list)

        df = self._rename_columns(df, column_rename_mapping={'address1': 'address'})

        if save_mode == 'parquet':
            df.to_parquet(filename_out)
            object_key = 's3://lg18dagbucket/to_warehouse/company_info_cleaned.parquet'
            self._save_data_in_s3(filename_out, object_key)

        elif save_mode == 'csv':
            df.to_csv(filename_out, index=False)
            object_key = 's3://lg18dagbucket/to_warehouse/company_info_cleaned.csv'
            self._save_data_in_s3(filename_out, object_key)

        return df
