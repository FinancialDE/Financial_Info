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
        self.aws_s3_bucket = 'testingbucketteam18'
        self.aws_rds_host = os.getenv('AWS_RDS_HOST')
        self.aws_rds_port = os.getenv('AWS_RDS_PORT')
        self.aws_rds_db_name = os.getenv('AWS_RDS_DB_NAME')
        self.aws_rds_user = os.getenv('AWS_RDS_DB_USER')
        self.aws_rds_password = os.getenv('AWS_RDS_DB_PASSWORD')

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

    def _save_raw_data_in_s3(self, raw_data, symbol):
        s3_client = boto3.client('s3', aws_access_key_id= self.aws_access_key_id, aws_secret_access_key= self.aws_secret_access_key)
        # Create a temporary file
        with tempfile.NamedTemporaryFile(suffix='.json') as temp_file:
            # Write the JSON data to the temporary file
            temp_file.write(raw_data.content)
            temp_file.flush()

            # Upload the temporary file to Amazon S3
            object_key = f'{symbol}.json'  # Customize the object key as needed
            s3_client.upload_file(temp_file.name, self.aws_s3_bucket, object_key)
