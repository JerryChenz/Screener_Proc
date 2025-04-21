import yfinance as yf
import pandas as pd
import time
import os
import datetime

"""
Script to scrape financial data from Yahoo Finance for a list of stock tickers and save it to a CSV file.

This script uses the `yfinance` library to fetch financial data for each ticker in the provided list.
It organizes the data into a pandas DataFrame and saves it to a CSV file in the `data/scraped_data` directory.
The script includes error handling with retries and processes tickers in batches to avoid overloading the API.

Input:
    - A list of stock tickers (e.g., ['AAPL', 'MSFT', 'GOOGL']).

Output: - A CSV file named `{csv_name}_YYYYMMDDHHMMSS.csv` (e.g., `scraped_data_20231015123456.csv`) containing the 
scraped financial data for each ticker. - The CSV file is saved in the `data/scraped_data` directory relative to the 
current working directory. - A list of any tickers that failed to process is printed to the console.

Important Notes:
    - The script retries failed requests up to 3 times with a 10-second wait between retries.
    - Tickers are processed in batches of 10 with a 1-second delay between batches to prevent API overload.
    - The script uses `None` for missing numerical data and `'N/A'` for missing strings to ensure data integrity.
    - The financial data retrieved includes:
        - Ticker
        - Company Name
        - Industry
        - Market Price
        - Market Currency
        - Report Currency
        - Market Capitalization
        - Financial Year End Date
        - Past Financial Year Dividends
        - Latest Invested Capital
        - Latest Total Debt
        - Latest Total Asset
        - Latest Common Equity
        - Past Annual Sales
        - Past Annual Cost of Goods Sold
        - Past Annual Operating Expenses
        - Past Annual Net Income
        - Past Annual Operating Cash Flow
        - Past Annual Financing Cash Flow
        - Past Annual Investing Cash Flow
"""


def get_ticker_data(ticker, retries=3, wait_time=10):
    """
    Fetch financial data for a single ticker from Yahoo Finance.
    Retries on failure with a delay.

    Args:
        ticker (str): Stock ticker symbol.
        retries (int): Number of retry attempts (default: 3).
        wait_time (int): Seconds to wait between retries (default: 10).

    Returns:
        dict: Financial data for the ticker, or None if fetching fails.
    """
    for attempt in range(retries):
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            # Initialize data dictionary with basic info
            data = {
                'Ticker': ticker,
                'Company Name': info.get('shortName', 'N/A'),
                'Industry': info.get('industry', 'N/A'),
                'Market Price': info.get('currentPrice', None),
                'Market Currency': info.get('currency', 'N/A'),
                'Report Currency': info.get('financialCurrency', 'N/A'),
                'Market Cap': info.get('marketCap', None)
            }
            # Extract financial year-end date
            last_fiscal_end = info.get('lastFiscalYearEnd')
            data['Financial Year End Date'] = (
                datetime.datetime.fromtimestamp(last_fiscal_end).strftime('%m-%d')
                if last_fiscal_end
                else 'N/A'
            )
            # Use trailing annual dividend rate for past year dividends
            data['Past Financial Year Dividends'] = info.get('trailingAnnualDividendRate', 0)
            # Get latest balance sheet data
            balance_sheet = stock.balance_sheet
            if not balance_sheet.empty:
                latest_date = balance_sheet.columns.max()
                latest_balance = balance_sheet[latest_date]
                data['Latest Invested Capital'] = latest_balance.get('Invested Capital', None)
                data['Latest Total Debt'] = latest_balance.get('Total Debt', None)
                data['Latest Total Asset'] = latest_balance.get('Total Assets', None)
                data['Latest Common Equity'] = latest_balance.get('Common Stock Equity', None)
            else:
                data['Latest Invested Capital'] = None
                data['Latest Total Debt'] = None
                data['Latest Total Asset'] = None
                data['Latest Common Equity'] = None
            # Get latest financials (sales and net income)
            financials = stock.financials
            if not financials.empty:
                latest_date = financials.columns.max()
                latest_financials = financials[latest_date]
                data['Past Annual Sales'] = latest_financials.get('Total Revenue', None)
                data['Past Annual Cogs'] = latest_financials.get('Cost Of Revenue', None)
                # Operating Expenses: Calculate if direct key not found
                opex = latest_financials.get('Total Operating Expenses', None)
                if opex is None:
                    gross_profit = latest_financials.get('Gross Profit', None)
                    operating_income = latest_financials.get('Operating Income', None)
                    if gross_profit is not None and operating_income is not None:
                        opex = gross_profit - operating_income  # Derived
                data['Past Annual Opex'] = opex
                data['Past Annual Net Income'] = latest_financials.get('Net Income', None)
            else:
                data['Past Annual Sales'] = None
                data['Past Annual Cogs'] = None
                data['Past Annual Opex'] = None
                data['Past Annual R&D'] = None
                data['Past Annual Net Income'] = None
            # Get latest cash flow data
            cashflow = stock.cashflow
            if not cashflow.empty:
                latest_date = cashflow.columns.max()
                latest_cashflow = cashflow[latest_date]
                data['Past Annual Operating Cash Flow'] = latest_cashflow.get('Operating Cash Flow', None)
                data['Past Annual Financing Cash Flow'] = latest_cashflow.get('Financing Cash Flow', None)
                data['Past Annual Investing Cash Flow'] = latest_cashflow.get('Investing Cash Flow', None)
            else:
                data['Past Annual Operating Cash Flow'] = None
                data['Past Annual Financing Cash Flow'] = None
                data['Past Annual Investing Cash Flow'] = None
            return data
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {ticker}: {e}")
            if attempt < retries - 1:
                time.sleep(wait_time)
            else:
                print(f"Failed to fetch data for {ticker} after {retries} attempts.")
                return None


def scrape_tickers(tickers, batch_size=10, retries=3, wait_time=10):
    """
    Scrape data for a list of tickers in batches.

    Args:
        tickers (list): List of stock ticker symbols.
        batch_size (int): Number of tickers per batch (default: 10).
        retries (int): Number of retry attempts per ticker (default: 3).
        wait_time (int): Seconds to wait between retries (default: 10).

    Returns:
        tuple: (pandas DataFrame of scraped data, list of failed tickers)
    """
    all_data = []
    failed_tickers = []
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        for ticker in batch:
            print(f"processing {ticker}...")
            data = get_ticker_data(ticker, retries, wait_time)
            if data:
                all_data.append(data)
                print(f"{ticker} done.")
            else:
                failed_tickers.append(ticker)
                print(f"{ticker} failed.")
        time.sleep(1)  # Delay between batches to avoid overloading API
    return pd.DataFrame(all_data), failed_tickers


def scrape_yfinance(tickers, csv_name='scraped_data'):
    """
    Main function to scrape data and save it to a CSV file.

    Args:
        tickers (list): List of stock ticker symbols.
        csv_name (str): Base name for the output CSV file (default: 'scraped_data').
    """
    # Scrape data
    df, failed_tickers = scrape_tickers(tickers)

    # Define output directory and ensure it exists
    output_dir = os.path.join(os.getcwd(), 'data', 'scraped_data')
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    output_file = os.path.join(output_dir, f'{csv_name}_{timestamp}.csv')

    # Save DataFrame to CSV
    df.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}")

    # Report failed tickers
    if failed_tickers:
        print("Failed to fetch data for the following tickers:")
        for ticker in failed_tickers:
            print(f" - {ticker}")
    else:
        print("All tickers processed successfully.")
