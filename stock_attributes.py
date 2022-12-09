import yfinance
from scrap_data import *


class Stock:
    def __init__(self, security_code):
        self.security_code = security_code
        self.price = None
        self.shares = None
        self.market_cap = None
        self.is_df = None
        self.bs_df = None

    def load_from_yf(self):
        """Scrap the data from yahoo finance"""

        ticker_data = yfinance.Ticker(self.security_code)

        self.price = [ticker_data.info['currentPrice'], ticker_data.info['currency']]
        self.shares = ticker_data.info['sharesOutstanding']
        self.market_cap = self.price * self.shares
        self.is_df = get_income_statement(self.security_code)
        self.bs_df = get_balance_sheet(self.security_code)

    def get_ev(self):
        """EV Formula = Market capitalization + Preferred stock + Outstanding debt + Minority interest – Cash and cash
        equivalents. """
        pass
