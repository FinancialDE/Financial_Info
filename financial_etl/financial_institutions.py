# The `FinancialInstitutions` class fetches data from the SEC website for financial institutions based on SIC codes and
# provides methods to access and manipulate the data.
from cik_to_ticker import CikToTicker
import time
import requests
import pandas as pd
import os
from bs4 import BeautifulSoup
import re
import sys
sys.path.append('../')

class FinancialInstitutions():
    def __init__(self):
        self.email = os.getenv('SEC_EMAIL')
        self.company_name = 'C1'
        self.headers = {"User-Agent": f"{self.company_name} {self.email}"}
        top_level_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        sic_codes_path = os.path.join(top_level_dir, 'data', 'SIC_Codes.xlsx')
        self.SIC_list = pd.read_excel(sic_codes_path)
        self.SIC_finance_list = self.SIC_list[self.SIC_list['Office'].str.contains('Finance|Crypto')]['SIC Code']
        self.site_data = {}
        self.to_concat = {}
        self.total_data = None
        self.last_request = time.time()

    def fetch_data(self, save_to_csv = True):
        for sic in self.SIC_finance_list:
            for i in range(0, 3000, 40):
                start_count = i
                print(f'Fetching SIC {sic} starting at {start_count}')
                base_url = f'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&SIC={sic}&owner=include&match=starts-with&start={start_count}&count=100&hidefilings=0'
                r = requests.get(base_url, headers=self.headers)
                temp_html = r.text
                now = time.time()
                if now - self.last_request < 1:
                    print('sleeping')
                    time.sleep(1 - (now - self.last_request))
                self.last_request = now
                try:
                    self.site_data[f'SIC_{sic}_{start_count}'] = pd.read_html(temp_html)
                    self.to_concat[f'SIC_{sic}_{start_count}'] = self.site_data[f'SIC_{sic}_{start_count}'][0]
                except ImportError:
                    self.site_data[f'SIC_{sic}_{start_count}'] = None
                    break

        self.total_data = pd.concat(self.to_concat.values(), axis=0)

        # Add a new column for ticker symbol
        cik_list = self.total_data['CIK'].tolist()
        cik_to_ticker_service = CikToTicker(self.headers)

        df_tickers_to_CIK = cik_to_ticker_service.getCIKtoTickerDF()
        for cik in cik_list:
            cik_st = str(cik).replace('.0', '')
            ticker = cik_to_ticker_service.CIKtoTicker(cik_st, df_tickers_to_CIK)
            if ticker:
                self.total_data.loc[self.total_data['CIK'] == cik, 'Ticker'] = ticker
            else:
                self.total_data.loc[self.total_data['CIK'] == cik, 'Ticker'] = None

        # Filter out rows where the Ticker column is None
        self.total_data = self.total_data[self.total_data['Ticker'].notnull()]
        print('Total data after filtering out rows with no ticker: ')
        print(self.count())
        if self.total_data is not None and save_to_csv == True:
          tickers = self.total_data['Ticker'].apply(lambda x: re.sub(r'\d+', '', x))
          tickers.to_csv('data/bank_list.csv', index=False)

    def all(self):
        return self.total_data

    def subset(self):
        return self.total_data.head(20)

    def count(self):
        return self.total_data.shape[0]

    def tickers(self):
      return self.total_data['Ticker'].tolist()

# Example usage:
sec_institutions = FinancialInstitutions()
sec_institutions.fetch_data()

print(sec_institutions.tickers())
