import time
import requests
import pandas as pd
import os
from bs4 import BeautifulSoup

class FinancialInstitutions():
    def __init__(self):
        self.email = os.getenv('SEC_EMAIL')
        self.company_name = 'C1'
        self.headers = {"User-Agent": f"{self.company_name} {self.email}"}
        self.SIC_list = pd.read_excel('financial_etl/SIC_Codes.xlsx')
        self.SIC_finance_list = self.SIC_list[self.SIC_list['Office'].str.contains('Finance|Crypto')]['SIC Code']
        self.site_data = {}
        self.to_concat = {}
        self.total_data = None
        self.last_request = time.time()

    def fetch_data(self):
        # for sic in self.SIC_finance_list:
        #     for i in range(0, 3000, 100):
        #         start_count = i
        #         print(f'Fetching SIC {sic} starting at {start_count}')
        #         base_url = f'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&SIC={sic}&owner=include&match=starts-with&start={start_count}&count=100&hidefilings=0'
        #         r = requests.get(base_url, headers=self.headers)
        #         temp_html = r.text

        #         try:
        #             self.site_data[f'SIC_{sic}_{start_count}'] = pd.read_html(temp_html)
        #             self.to_concat[f'SIC_{sic}_{start_count}'] = self.site_data[f'SIC_{sic}_{start_count}'][0]
        #         except ImportError:
        #             self.site_data[f'SIC_{sic}_{start_count}'] = None
        #             break
        sic = 6022
        base_url = f'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&SIC={sic}&owner=include&match=starts-with&start=40&count=100&hidefilings=0'
        r = requests.get(base_url, headers=self.headers)
        temp_html = r.text

        try:
            self.site_data[f'SIC_{sic}'] = pd.read_html(temp_html)
            self.to_concat[f'SIC_{sic}'] = self.site_data[f'SIC_{sic}'][0]
        except ImportError:
            self.site_data[f'SIC_{sic}'] = None
        self.total_data = pd.concat(self.to_concat.values(), axis=0)

        # Add a new column for ticker symbol
        cik_list = self.total_data['CIK'].tolist()
        print(self.total_data)
        for cik in cik_list:
            cik_with_zeros = str(cik).zfill(10)
            url = f'https://data.sec.gov/submissions/CIK{cik_with_zeros}.json'

            response = requests.get(url, headers=self.headers)
            data = response.json()
            now = time.time()
            if now - self.last_request < 1:
                print('sleeping')
                time.sleep(1 - (now - self.last_request))
            self.last_request = now
            if response.status_code == 200 and data['tickers']:
                self.total_data.loc[self.total_data['CIK'] == cik, 'Ticker'] = data['tickers'][0]
            else:
                self.total_data.loc[self.total_data['CIK'] == cik, 'Ticker'] = None

        # Filter out rows where the Ticker column is None
        self.total_data = self.total_data[self.total_data['Ticker'].notnull()]
        if self.total_data is not None:
          self.total_data.to_csv('data/bank_list.csv')

    def all(self):
        return self.total_data

    def subset(self):
        return self.total_data.head(20)

    def count(self):
          return self.total_data.shape[0]

# Example usage:
sec_institutions = FinancialInstitutions()
sec_institutions.fetch_data()
print(sec_institutions.all())
