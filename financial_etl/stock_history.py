import pandas as pd
import requests
import csv
import mysql.connector
import os
import json
import boto3
import io
import psycopg2
from sqlalchemy import create_engine
from selenium import webdriver
from financial_etl.base import Base_ETL

class StockHistory_ETL(Base_ETL):
    '''Stock History ETL Pipeline'''

    def __init__(self):
        super().__init__()  # Call the parent class __init__ method
        self._APIkey = os.getenv('STOCK_API_KEY')
        self.url_AlphaVtg = 'https://www.alphavantage.co/query'
        self._object_key = os.getenv('OBJECT_KEY')
        self._raw_object_key = os.getenv('RAW_OBJECT_KEY')
        #Redshift Arguments
        self._redshift_endpoint = os.getenv('REDSHIFT_ENDPOINT')
        self._redshift_port = os.getenv('REDSHIFT_PORT')
        self._redshift_db_name = os.getenv('REDSHIFT_DB_NAME')
        self._redshift_user = os.getenv('REDSHIFT_USER')
        self._redshift_password = os.getenv('REDSHIFT_PASSWORD')
        
        #S3 Arguments
        self._bucket_name = os.getenv('BUCKET_NAME')
        self._object_key = os.getenv('OBJECT_KEY')

    def extract(self, symbol, filename_out=None):
        ''' Extract Stock Market Data with AlphaVantage API given symbol of the company
            Args:
                symbol: 'JPM' (JPMorgan Chase & Co.)
                        'GS' (The Goldman Sachs Group, Inc.)
                        'MS' (Morgan Stanley)
                        'SIVBQ' (SVB Financial Group)
            Returns:
                json for the stock history information
        '''

        params = {
                'function': 'TIME_SERIES_DAILY_ADJUSTED',
                'symbol': symbol,
                'outputsize': 'full',
                'apikey': self._APIkey}

        raw_data = requests.get(self.url_AlphaVtg, params=params)
        
        if filename_out is None:
            if not os.path.exists(self.dir_data_lake):
                os.makedirs(self.dir_data_lake)

            filename_out = os.path.join(self.dir_data_lake, symbol+'.json')

        self.save_to_json(raw_data.json(), filename_out)

    def _filter_by_date_range(self, df, date_range):
        df = df.loc[df['date'].isin(pd.date_range(start=date_range[0], end=date_range[1]))]
        return df

    def _get_is_quarter(self, dates):

        dates = pd.to_datetime(dates)

        return dates.dt.is_quarter_end

    def _get_is_annual(self, dates):

        dates = pd.to_datetime(dates)

        is_annual = (dates.dt.month == 12) & (dates.dt.day == 31)

        return is_annual


    def transform_single_firm(self, symbol, dir_data_lake):
        

        filename = os.path.join(dir_data_lake, symbol+'.json')
        with open(filename, "r") as file:
            raw_data = json.load(file)

        data = raw_data['Time Series (Daily)']
        df = pd.DataFrame.from_dict(data, orient='index')
        df.index = pd.to_datetime(df.index)

        # --- Transform Steps ---

        # ------ 1) modify column names ------
        df.rename(columns={df.columns[colID]:df.columns[colID][3:] for colID in range(df.shape[1])}, inplace=True)
        df = df.reset_index()

        column_rename_mapping = {'index': 'date', 'open': 'open_price', 'close': 'close_price', 'high': 'highest_price', 'low': 'lowest_price'}

        df = df.rename(columns=column_rename_mapping)

        # ------ 2) drop non-important columns ------
        df = df.drop('adjusted close', axis=1)
        df = df.drop('dividend amount', axis=1)
        df = df.drop('split coefficient', axis=1)

        # ------ 3) filter the date range ------
        df = self._filter_by_date_range(df, date_range=['2017-01-01', '2022-03-31'])


        # ------ 4) add relavent information as new columns ------
        symbol = raw_data['Meta Data']['2. Symbol']
        df['symbol'] = [symbol]*len(df)
        df['is_quarter'] = self._get_is_quarter(dates=df.date)
        df['is_annual'] = self._get_is_annual(dates=df.date)

        return df
    
    def transform(self, symbols, dir_data_lake, filename_out=None, save_mode='csv'):
        
        bucket_name = self._bucket_name
        s3_client = boto3.client('s3')
        if filename_out is None:
            filename_out = os.path.join(self.dir_data, 'stock_history.csv')

        print(filename_out)
        df_list = []
        for symbol in symbols:
        
            df = self.transform_single_firm(symbol, dir_data_lake)
            df_list.append(df)
    
        joined_df = self.concat_dataframes(df_list)

        if save_mode == 'parquet':
            joined_df.to_parquet(filename_out)
            object_key = self._object_key + 'stock_history_cleaned.parquet'
            s3_client.upload_file(filename_out, bucket_name, object_key)
        elif save_mode == 'csv':
            joined_df.to_csv(filename_out, index=False)
            object_key = self._object_key + 'stock_history_cleaned.csv'
            s3_client.upload_file(filename_out, bucket_name, object_key)


        return joined_df

    def load(self):
        """ This function creates the s3 and redshift connection,
            creates the stock_history table and loads the transformed table into Redshift."""
        bucket_name = self._bucket_name

        object_key = self._object_key + "stock_history_cleaned.csv" 
         
        REDSHIFT_ENDPOINT = self._redshift_endpoint 
        REDSHIFT_PORT = self._redshift_port
        REDSHIFT_DB_NAME = self._redshift_db_name 
        REDSHIFT_USER = self._redshift_user
        REDSHIFT_PASSWORD = self._redshift_password
        engine =  create_engine(f'redshift+redshift_connector://{REDSHIFT_USER}:{REDSHIFT_PASSWORD}@{REDSHIFT_ENDPOINT}:{REDSHIFT_PORT}/{REDSHIFT_DB_NAME}')
         
        # Delete existing table named "balance_sheet"
        sql_query = """DROP TABLE IF EXISTS stock_history"""
         
        with engine.connect() as connection:
             connection.execute(sql_query)
         
         
        # Create a new table named "balance_sheet"
        sql_query = """CREATE TABLE IF NOT EXISTS stock_history 
                        (
                          address VARCHAR(255),
                          city VARCHAR(100),
                          state VARCHAR(50),
                          zip INT,
                          country VARCHAR(100),
                          phone VARCHAR(20),
                          website VARCHAR(255),
                          industry VARCHAR(100),
                          sector VARCHAR(100),
                          long_business_summary VARCHAR(MAX),
                          full_time_employees INT,
                          symbol VARCHAR(10)
                        )
                     """
        #execute_sql(sql_query, conn_string)
        
        with engine.begin() as connection:
             connection.execute(sql_query) 
        
        # Load SQL Data into S3
        conn = psycopg2.connect(
        dbname=self._redshift_db_name,
        user=self._redshift_user,
        password=self._redshift_password,
        host=self._redshift_endpoint,
        port=self._redshift_port
        )
    
        cursor = conn.cursor()
        
        copy_command = f"""
    COPY dev.public.stock_history
    FROM 's3://lg18dagbucket/s3://lg18dagbucket/to_warehouse/stock_history_cleaned.csv'
    IAM_ROLE 'arn:aws:iam::380332205208:role/service-role/AmazonRedshift-CommandsAccessRole-20230708T074227'
    FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'us-east-1';
"""

        cursor.execute(copy_command)
        conn.commit()
        
        cursor.close()
        conn.close()
    
    def remove_files(self):
        data_lake_dir = os.path.join(os.path.dirname(__file__), '..', 'data_lake')
        data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    
        # Remove files in the data_lake directory
        for filename in os.listdir(data_lake_dir):
            file_path = os.path.join(data_lake_dir, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
    
        # Remove files in the data directory
        for filename in os.listdir(data_dir):
            file_path = os.path.join(data_dir, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
