import yahooquery as yq
import pandas as pd
import time
import os
import datetime


def safe_get(data, *keys, default='N/A'):
    """
    Safely access nested dictionary keys.
    Args:
        data: The starting data object (usually a dictionary).
        *keys: Variable number of keys to traverse.
        default: Value to return if access fails (default: 'N/A').
    Returns:
        The value at the nested key path or the default if unavailable.
    """
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key)
            if data is None:
                return default
        else:
            return default
    return data if data is not None else default


def get_tickers_data(tickers, retries=3, wait_time=10):
    """
    Fetch financial data for a batch of tickers using yahooquery.
    Retries on failure with a delay.

    Args:
        tickers (list): List of stock ticker symbols.
        retries (int): Number of retry attempts (default: 3).
        wait_time (int): Seconds to wait between retries (default: 10).

    Returns:
        list: List of dictionaries containing financial data for each ticker.
    """
    for attempt in range(retries):
        try:
            # Create a yahooquery Ticker object for the batch
            tickers_obj = yq.Ticker(tickers)
            data_list = []

            # Fetch all modules data for company details and market data
            all_data = tickers_obj.all_modules

            # Fetch financial statements
            balance_sheet = tickers_obj.balance_sheet(frequency='a', trailing=False)
            income_statement = tickers_obj.income_statement(frequency='a', trailing=False)
            cash_flow_data = tickers_obj.cash_flow(frequency='a', trailing=False)

            for ticker in tickers:
                ticker_data = all_data.get(ticker, {})
                if not ticker_data or not isinstance(ticker_data, dict):
                    print(f"No data found for {ticker}")
                    data_list.append({'Ticker': ticker})
                    continue

                # Initialize data dictionary with existing fields
                data = {
                    'Ticker': ticker,
                    'Company Name': safe_get(ticker_data, 'quoteType', 'shortName'),
                    'Industry': safe_get(ticker_data, 'assetProfile', 'industry'),
                    'Market Price': safe_get(ticker_data, 'price', 'regularMarketPrice', 'raw', default=None),
                    'Market Currency': safe_get(ticker_data, 'price', 'currency'),
                    'Market Capitalization': safe_get(ticker_data, 'price', 'marketCap', 'raw', default=None),
                    'Past Financial Year Dividends': safe_get(ticker_data, 'summaryDetail', 'trailingAnnualDividendRate', 'raw', default=0),
                }

                # Financial Year End Date
                calendar_events = ticker_data.get('calendarEvents', {})
                earnings = calendar_events.get('earnings', {})
                data['Financial Year End Date'] = safe_get(earnings, 'earningsDate', 0, 'raw', default='N/A')

                # Balance Sheet Data
                if ticker in balance_sheet.index:
                    latest_balance = balance_sheet.loc[ticker].iloc[-1]
                    data['Latest Invested Capital'] = latest_balance.get('investedCapital', None)
                    data['Latest Total Debt'] = latest_balance.get('totalDebt', None)
                    data['Latest Total Assets'] = latest_balance.get('totalAssets', None)
                    data['Latest Common Equity'] = latest_balance.get('commonStockEquity', None)
                else:
                    data['Latest Invested Capital'] = None
                    data['Latest Total Debt'] = None
                    data['Latest Total Assets'] = None
                    data['Latest Common Equity'] = None

                # Income Statement Data
                if ticker in income_statement.index:
                    latest_income = income_statement.loc[ticker].iloc[-1]
                    data['Past Annual Sales (Total Revenue)'] = latest_income.get('totalRevenue', None)
                    data['Past Annual Cost of Goods Sold (COGS)'] = latest_income.get('costOfRevenue', None)
                    data['Past Annual Operating Expenses'] = latest_income.get('totalOperatingExpenses', None)
                    data['Past Annual Net Income'] = latest_income.get('netIncome', None)
                else:
                    data['Past Annual Sales (Total Revenue)'] = None
                    data['Past Annual Cost of Goods Sold (COGS)'] = None
                    data['Past Annual Operating Expenses'] = None
                    data['Past Annual Net Income'] = None

                # Cash Flow Data (New Addition)
                if ticker in cash_flow_data.index:
                    latest_cashflow = cash_flow_data.loc[ticker].iloc[-1]
                    data['Past Annual Operating Cash Flow'] = latest_cashflow.get('totalCashFromOperatingActivities', None)
                    data['Past Annual Financing Cash Flow'] = latest_cashflow.get('totalCashFromFinancingActivities', None)
                    data['Past Annual Investing Cash Flow'] = latest_cashflow.get('totalCashFromInvestingActivities', None)
                else:
                    data['Past Annual Operating Cash Flow'] = None
                    data['Past Annual Financing Cash Flow'] = None
                    data['Past Annual Investing Cash Flow'] = None

                data_list.append(data)

            return data_list

        except Exception as e:
            print(f"Batch attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(wait_time)
            else:
                print(f"Failed to fetch batch data after {retries} attempts.")
                return [{'Ticker': t} for t in tickers]


def scrape_tickers(tickers, batch_size=10, retries=3, wait_time=10):
    """
    Scrape data for a list of tickers in batches.

    Args:
        tickers (list): List of stock ticker symbols.
        batch_size (int): Number of tickers per batch (default: 10).
        retries (int): Number of retry attempts per batch (default: 3).
        wait_time (int): Seconds to wait between retries (default: 10).

    Returns:
        tuple: (pandas DataFrame of scraped data, list of failed tickers)
    """
    all_data = []
    failed_tickers = []

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        data_list = get_tickers_data(batch, retries, wait_time)

        for data in data_list:
            ticker = data['Ticker']
            # Check if data is incomplete (only Ticker field present)
            if len(data) <= 1:
                failed_tickers.append(ticker)
            all_data.append(data)

        time.sleep(1)  # Delay between batches to avoid overloading API

    return pd.DataFrame(all_data), failed_tickers


def scrape_yahooquery(tickers, csv_name='scraped_data'):
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
