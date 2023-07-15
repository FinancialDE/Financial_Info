
# The `CikToTicker` class is a Python class that provides methods for converting CIK (Central Index Key) codes to ticker
# symbols using data from the SEC (Securities and Exchange Commission).

# Code sourced from: https://github.com/FinancicalDE/financial_ratios/blob/main/tickers.py authored by @Eryam

import requests
import pandas as pd

class CikToTicker():
    def __init__(self, headers=None):
        self.headers = headers

    def getCIKtoTickerDF(self, leadingZeros=0, tickerAsIndex=False):
        df_tickers_to_CIK = self._fetch_tickers_to_CIK()
        df_tickers_to_CIK = self._normalize_tickers_to_CIK(df_tickers_to_CIK)
        df_tickers_to_CIK = self._format_cik_str(df_tickers_to_CIK, leadingZeros)
        df_tickers_to_CIK = self._reset_index(df_tickers_to_CIK)
        if tickerAsIndex:
            df_tickers_to_CIK = self._set_ticker_as_index(df_tickers_to_CIK)
        return df_tickers_to_CIK

    def CIKtoTicker(self, CIK, df_tickers_to_CIK=None):
        if df_tickers_to_CIK is None:
            df_tickers_to_CIK = self.getCIKtoTickerDF()

        ticker = self._get_ticker_by_CIK(df_tickers_to_CIK, CIK)
        if ticker is None:
            return None
        return ticker

    def _fetch_tickers_to_CIK(self):
        response = requests.get("https://www.sec.gov/files/company_tickers.json", headers=self.headers)
        return response.json()

    def _normalize_tickers_to_CIK(self, tickers_to_CIK):
        df_tickers_to_CIK = pd.json_normalize(pd.json_normalize(tickers_to_CIK, max_level=0).values[0])
        return df_tickers_to_CIK

    def _format_cik_str(self, df_tickers_to_CIK, leadingZeros):
        if leadingZeros == 0:
            df_tickers_to_CIK["cik_str"] = df_tickers_to_CIK['cik_str'].astype(str)
        else:
            df_tickers_to_CIK["cik_str"] = df_tickers_to_CIK['cik_str'].astype(str).str.zfill(10)
        return df_tickers_to_CIK

    def _reset_index(self, df_tickers_to_CIK):
        df_tickers_to_CIK = df_tickers_to_CIK.reset_index()
        return df_tickers_to_CIK

    def _set_ticker_as_index(self, df_tickers_to_CIK):
        df_tickers_to_CIK = df_tickers_to_CIK.set_index('ticker')
        return df_tickers_to_CIK

    def _get_ticker_by_CIK(self, df_tickers_to_CIK, CIK):
        ticker = df_tickers_to_CIK.loc[df_tickers_to_CIK['cik_str'] == CIK, 'ticker']
        if ticker.empty:
            return None
        return ticker.values[0]


# example usage:
# cik_to_ticker_service = CikToTicker({'User-Agent': 'testing'})
# print(cik_to_ticker_service.CIKtoTicker('320193'))
