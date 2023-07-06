import pandas as pd
import requests
import csv
import mysql.connector
import os
import json
from financial_etl.base import Base_ETL

class StockHistory_ETL(Base_ETL):
    '''Stock History ETL Pipeline'''

    def __init__(self):
        super().__init__()  # Call the parent class __init__ method
        self._APIkey = os.getenv('STOCK_API_KEY')
        self.url_AlphaVtg = 'https://www.alphavantage.co/query'

        self.dir_thisfile = os.path.dirname(os.path.abspath(__file__))
        self.dir_repo = os.path.join(self.dir_thisfile, '../')
        self.dir_data_lake = os.path.join(self.dir_repo, 'data_lake')
        self.dir_data = os.path.join(self.dir_repo, 'data')

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
    
    def transform(self, symbols, dir_data_lake, filename_out=None):
        
        if filename_out is None:
            filename_out = os.path.join(self.dir_data, 'stock_history.csv')

        df_list = []
        for symbol in symbols:
        
            df = self.transform_single_firm(symbol, dir_data_lake)
            df_list.append(df)
    
        joined_df = self.concat_dataframes(df_list)
        self.save_to_csv(joined_df, filename=filename_out)

        return joined_df

    def load(self, file_csv, table_name):
        '''Load CSV data to SQL Database'''

        # Connect to the AWS RDS database
        conn = mysql.connector.connect(
            host=self.aws_rds_host,
            port=self.aws_rds_port,
            database=self.aws_rds_db_name,
            user=self.aws_rds_user,
            password=self.aws_rds_password
        )

        # Create a cursor object
        cursor = conn.cursor()

        # Create the table if it doesn't exist
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                date TEXT,
                symbol TEXT,
                open_price FLOAT,
                high_price FLOAT,
                low_price FLOAT,
                close_price FLOAT,
                volume INT,
                is_quarter BOOLEAN,
                is_annual BOOLEAN
            );
        ''')

        with open(file_csv, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row
            data = [tuple(row) for row in reader]  # Convert rows to tuples

        cursor.executemany(f'''
            INSERT INTO {table_name} (date, symbol, open_price, high_price, low_price, close_price, volume, is_quarter, is_annual)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', data)  # Pass data as the second parameter

        conn.commit()  # Commit the changes to the database

        cursor.close()  # Close the cursor
        conn.close()  # Close the connection
