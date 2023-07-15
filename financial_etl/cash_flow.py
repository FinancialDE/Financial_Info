import os
from yahooquery import Ticker
from financial_etl.base import Base_ETL
import pandas as pd
import boto3
from selenium import webdriver
import io
from sqlalchemy import create_engine
import psycopg2
class CashFlow_ETL(Base_ETL):

    def __init__(self):
        super().__init__()
        self._username = os.getenv('YFINANCE_USER')
        self._password = os.getenv('YFINANCE_PASSWORD')
        self._object_key = os.getenv('OBJECT_KEY')
        self._raw_object_key = os.getenv('RAW_OBJECT_KEY')
        # Redshift Arguments
        self._redshift_endpoint = os.getenv('REDSHIFT_ENDPOINT')
        self._redshift_port = os.getenv('REDSHIFT_PORT')
        self._redshift_db_name = os.getenv('REDSHIFT_DB_NAME')
        self._redshift_user = os.getenv('REDSHIFT_USER')
        self._redshift_password = os.getenv('REDSHIFT_PASSWORD')

        # S3 Arguments
        self._bucket_name = os.getenv('BUCKET_NAME')
        self._object_key = os.getenv('OBJECT_KEY')
        self.columns_to_keep = ['symbol', 'date', 'OperatingCashFlow', 'InvestingCashFlow', 
                   'FinancingCashFlow', 'EndCashPosition', 'IncomeTaxPaidSupplementalData', 
                   'InterestPaidSupplementalData', 'CapitalExpenditure', 
                   'IssuanceOfCapitalStock', 'IssuanceOfDebt', 'RepaymentOfDebt', 
                   'RepurchaseOfCapitalStock', 'FreeCashFlow']

    def extract(self, symbols, filename_out=None):
        
        raw_data = Ticker(symbols, username=self._username, password=self._password).p_cash_flow(trailing=False, frequency='q')

        if filename_out is None:
            if not os.path.exists(self.dir_data_lake):
                os.makedirs(self.dir_data_lake)
            
            filename_out = os.path.join(self.dir_data_lake, 'cash_flow.csv')

        raw_data.to_csv(filename_out, index=True)
        bucket_name = self._bucket_name
        object_key = self._raw_object_key + 'cash_flow.csv' 

        s3_client = boto3.client('s3')
        s3_client.upload_file(filename_out, bucket_name, object_key)
    
    def _rename_columns(self, df, column_rename_mapping={'asOfDate': 'date'}):
        return df.rename(columns=column_rename_mapping)
    
    def _filter_by_date_range(self, df, date_range):
        df['date'] = pd.to_datetime(df['date'])
        df = df.loc[df['date'].isin(pd.date_range(start=date_range[0], end=date_range[1]))]
        return df.reset_index(drop=True)
    
    def transform(self, filename_in, filename_out, save_mode='csv'):

        #df_raw = pd.read_csv(filename_in)
        bucket_name = self._bucket_name
        object_key = self._raw_object_key + 'cash_flow.csv'

        #Set Local File Path
        local_file_path = filename_in
        #Create an S3 client
        s3_client = boto3.client('s3')

        # Upload the file to S3
        df_raw = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        df_raw = df_raw['Body'].read().decode('utf-8')
        df_raw = pd.read_csv(io.StringIO(df_raw))

        df = self._rename_columns(df_raw, column_rename_mapping={'asOfDate': 'date'})

        df = self._filter_by_date_range(df, date_range=['2017-01-01', '2022-03-31'])
        
        df = df[self.columns_to_keep]

        if save_mode == 'parquet':
            bucket_name = 'lg18dagbucket'
            object_key = 's3://lg18dagbucket/to_warehouse/cash_flow_cleaned.parquet'

            s3_client = boto3.client('s3')
            s3_client.upload_file(filename_out, bucket_name, object_key)
        
        elif save_mode == 'csv':
            df.to_csv(filename_out, index=False)
            bucket_name = 'lg18dagbucket'
            object_key = 's3://lg18dagbucket/to_warehouse/cash_flow_cleaned.csv'

            s3_client = boto3.client('s3')
            s3_client.upload_file(filename_out, bucket_name, object_key)
            

        return df

    def load(self):
        """ This function creates the s3 and redshift connection,
            creates the cash_flow table and loads the transformed table into Redshift."""
        bucket_name = self._bucket_name

        object_key = self._object_key + "cash_flow_cleaned.csv"

        REDSHIFT_ENDPOINT = self._redshift_endpoint
        REDSHIFT_PORT = self._redshift_port
        REDSHIFT_DB_NAME = self._redshift_db_name
        REDSHIFT_USER = self._redshift_user
        REDSHIFT_PASSWORD = self._redshift_password
        engine = create_engine(
            f'redshift+redshift_connector://{REDSHIFT_USER}:{REDSHIFT_PASSWORD}@{REDSHIFT_ENDPOINT}:{REDSHIFT_PORT}/{REDSHIFT_DB_NAME}')

        # Delete existing table named "cash_flow"
#        sql_query = """DROP TABLE IF EXISTS cash_flow_cleaned.csv"""
#
#        with engine.connect() as connection:
#            connection.execute(sql_query)

        # Create a new table named "cash_flow"
        sql_query = """CREATE TABLE IF NOT EXISTS cash_flow 
                        (
                        symbol VARCHAR,
                        date DATE,
                        OperatingCashFlow DECIMAL,
                        InvestingCashFlow DECIMAL,
                        FinancingCashFlow DECIMAL,
                        EndCashPosition DECIMAL,
                        IncomeTaxPaidSupplementalData DECIMAL,
                        InterestPaidSupplementalData DECIMAL,
                        CapitalExpenditure DECIMAL,
                        IssuanceOfCapitalStock DECIMAL,
                        IssuanceOfDebt DECIMAL,
                        RepaymentOfDebt DECIMAL,
                        RepurchaseOfCapitalStock DECIMAL,
                        FreeCashFlow DECIMAL
                    )
                    """
        # execute_sql(sql_query, conn_string)

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
    COPY dev.public.cash_flow
    FROM 's3://lg18dagbucket/s3://lg18dagbucket/to_warehouse/cash_flow_cleaned.csv'
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

