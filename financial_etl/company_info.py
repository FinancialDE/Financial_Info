import os
import json
from yahooquery import Ticker
from financial_etl.base import Base_ETL
import pandas as pd
import boto3
import io
from sqlalchemy import create_engine
import psycopg2
from selenium import webdriver
class CompanyInfo_ETL(Base_ETL):

    def __init__(self):
        super().__init__()
        self._username = os.getenv('YFINANCE_USER')
        self._password = os.getenv('YFINANCE_PASSWORD')
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

    def extract(self, symbols, filename_out=None):
        '''Extract Company Info with Yfianace API

            Args:
                symbols = ['JPM', 'GS', 'MS', 'SIVBQ']

            Returns:
                pd.DataFrame
        '''

        raw_data = Ticker(symbols=symbols, username=self._username, password=self._password).asset_profile
        print(raw_data)
        if filename_out is None:
            if not os.path.exists(self.dir_data_lake):
                os.makedirs(self.dir_data_lake)

            filename_out = os.path.join(self.dir_data_lake, 'company_info.json')
        self.save_to_json(raw_data, filename_out)
        object_key = 's3://lg18dagbucket/STAGING/company_info.json'
        self._save_data_in_s3(filename_out, object_key)

        bucket_name = self._bucket_name
        object_key = self._raw_object_key + 'company_info.json'

        s3_client = boto3.client('s3')
        s3_client.upload_file(filename_out, bucket_name, object_key)

    def _rename_columns(self, df, column_rename_mapping={'asOfDate': 'date'}):
        return df.rename(columns=column_rename_mapping)

    def transform(self, filename_in, filename_out, save_mode='parquet'):

        s3_client = boto3.client('s3')

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

            bucket_name = self._bucket_name
            object_key = self._object_key + 'company_info_cleaned.parquet'
            s3_client.upload_file(filename_out, bucket_name, object_key)
        elif save_mode == 'csv':
            df.to_csv(filename_out, index=False)
            bucket_name = self._bucket_name
            object_key = self._object_key + 'company_info_cleaned.csv'
            s3_client.upload_file(filename_out, bucket_name, object_key)

        return df
    
    def load(self):
        """ This function creates the s3 and redshift connection,
            creates the company_info_sheet table and loads the transformed table into Redshift."""
        bucket_name = self._bucket_name

        object_key = self._object_key + "company_info_cleaned.csv"

        REDSHIFT_ENDPOINT = self._redshift_endpoint
        REDSHIFT_PORT = self._redshift_port
        REDSHIFT_DB_NAME = self._redshift_db_name
        REDSHIFT_USER = self._redshift_user
        REDSHIFT_PASSWORD = self._redshift_password
        engine =  create_engine(f'redshift+redshift_connector://{REDSHIFT_USER}:{REDSHIFT_PASSWORD}@{REDSHIFT_ENDPOINT}:{REDSHIFT_PORT}/{REDSHIFT_DB_NAME}')

        # Delete existing table named "company_info_sheet"
        sql_query = """DROP TABLE IF EXISTS company_info"""

        with engine.connect() as connection:
             connection.execute(sql_query)
        # Create a new table named "company_info_sheet"
        sql_query = """CREATE TABLE IF NOT EXISTS company_info
                        (
                        symbol                               VARCHAR,
                        date                                 VARCHAR,
                        TotalAssets                          NUMERIC,
                        TotalLiabilitiesNetMinorityInterest  NUMERIC,
                        TotalEquityGrossMinorityInterest     NUMERIC,
                        TotalCapitalization                  NUMERIC,
                        PreferredStockEquity                 NUMERIC,
                        CommonStockEquity                    NUMERIC,
                        NetTangibleAssets                    NUMERIC,
                        InvestedCapital                      NUMERIC,
                        TangibleBookValue                    NUMERIC,
                        TotalDebt                            NUMERIC,
                        NetDebt                              NUMERIC,
                        ShareIssued                          NUMERIC,
                        OrdinarySharesNumber                 NUMERIC,
                        PreferredSharesNumber                NUMERIC,
                        TreasurySharesNumber                 NUMERIC

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
    COPY dev.public.company_info
    FROM 's3://lg18dagbucket/s3://lg18dagbucket/to_warehouse/company_info_cleaned.csv'
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
