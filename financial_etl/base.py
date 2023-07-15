import os
import pandas as pd
import json
from dotenv import load_dotenv
import tempfile
import boto3
# Load environment variables from .env file
load_dotenv()

class Base_ETL():
    def __init__(self):
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.aws_s3_bucket = 'lg18dagbucket'
        self.aws_rds_host = os.getenv('AWS_RDS_HOST')
        self.aws_rds_port = os.getenv('AWS_RDS_PORT')
        self.aws_rds_db_name = os.getenv('AWS_RDS_DB_NAME')
        self.aws_rds_user = os.getenv('AWS_RDS_DB_USER')
        self.aws_rds_password = os.getenv('AWS_RDS_DB_PASSWORD')

        self._get_path_variables()

    def _get_path_variables(self):
        self.dir_thisfile = os.path.dirname(os.path.abspath(__file__))
        self.dir_repo = os.path.join(self.dir_thisfile, '../')
        self.dir_data_lake = os.path.join(self.dir_repo, 'data_lake')
        self.dir_data = os.path.join(self.dir_repo, 'data')

    def extract(self):
        '''Extract raw data based on used API'''
        pass

    def transform(self):
        '''Transform raw_data to targeted format'''
        pass

    def load(self):
        '''Load CSV data to SQL Database'''
        pass

    def concat_dataframes(self, df_list):
        '''Concatenate all the DataFrames into a single DataFrame'''
        joined_df = pd.concat(df_list, axis=0)
        return joined_df

    def save_to_csv(self, df, filename):
        '''Save the input DataFrame to a CSV file'''
        df.to_csv(filename, index=False)

    def save_to_json(self, data, filename):
        '''Save the input data to json file'''

        with open(filename, 'w') as file:
            json.dump(data, file)

    def _save_data_in_s3(self, filename_out, object_key):
        s3_client = boto3.client('s3')
        s3_client.upload_file(filename_out, self.bucket_name, object_key)

    def _filter_columns(self, df, drop_threshold=0.8):
        ''' Drop columns that have a high percentage of null values'''

        null_fractions = df.isnull().mean()
        columns_to_drop = null_fractions[null_fractions > drop_threshold].index

        print("Number of columns in the original DataFrame:", len(df.columns))
        print("Number of columns to be dropped:", len(columns_to_drop))
        print('Columns to drop:', columns_to_drop)

        return df.drop(columns=columns_to_drop)
