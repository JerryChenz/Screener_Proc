import pandas as pd
import os
import glob
import re
from yahooquery import Ticker
import time


def fetch_stock_data(tickers, retries=3, wait_time=10):
    """
    Fetch the current market prices and market capitalizations for a list of tickers using yahooquery.
    Returns a dictionary of ticker to {'price': price, 'market_cap': market_cap}, with None for failed fetches.
    """
    data = {}
    ticker_obj = Ticker(tickers)
    quotes = ticker_obj.quotes
    summary_detail = ticker_obj.summary_detail

    failed_tickers = []
    for ticker in tickers:
        quote = quotes.get(ticker)
        detail = summary_detail.get(ticker)
        if isinstance(quote, dict) and isinstance(detail, dict):
            price = quote.get('regularMarketPrice')
            market_cap = detail.get('marketCap')
            if price is not None and market_cap is not None:
                data[ticker] = {'price': price, 'market_cap': market_cap}
            else:
                failed_tickers.append(ticker)
        else:
            failed_tickers.append(ticker)

    # Retry failed tickers individually
    for ticker in failed_tickers:
        for attempt in range(retries):
            try:
                stock = Ticker(ticker)
                quote = stock.quotes[ticker]
                detail = stock.summary_detail[ticker]
                if isinstance(quote, dict) and isinstance(detail, dict):
                    price = quote.get('regularMarketPrice')
                    market_cap = detail.get('marketCap')
                    if price is not None and market_cap is not None:
                        data[ticker] = {'price': price, 'market_cap': market_cap}
                        break
                time.sleep(wait_time)
            except Exception as e:
                print(f"Attempt {attempt + 1} failed for {ticker}: {e}")
                if attempt < retries - 1:
                    time.sleep(wait_time)
                else:
                    print(f"Failed to fetch data for {ticker} after {retries} attempts.")
                    data[ticker] = None

    return data


def clean_scraped_data(skip=True):
    """
    Consolidate multiple scraped CSV files into three clean regional CSV files.

    Reads all CSV files in `data/scraped_data` with naming patterns `us*_[timestamp].csv`,
    `cn*_[timestamp].csv`, and `hk*_[timestamp].csv`. For each region, combines the data,
    keeping only the most recent entry per ticker based on the timestamp in the filename.
    Saves the consolidated data to `us_screen_data.csv`, `cn_screen_data.csv`, and
    `hk_screen_data.csv` in `data/cleaned_data`.

    If skip is False and the clean file exists, updates the 'Market Price' and 'Market Cap'
    columns efficiently using yahooquery, with retries for failed API calls.

    If no CSV files or missing one or all regional CSV files, skips that region.

    Returns:
        None
    """
    # Define directories
    scraped_dir = os.path.join(os.getcwd(), 'data', 'scraped_data')
    clean_dir = os.path.join(os.getcwd(), 'data', 'cleaned_data')

    # Ensure directories exist
    if not os.path.exists(scraped_dir):
        print(f"Error: {scraped_dir} does not exist.")
        return
    os.makedirs(clean_dir, exist_ok=True)

    # Define region patterns
    regions = {
        'us': 'us*_*.csv',
        'cn': 'cn*_*.csv',
        'hk': 'hk*_*.csv'
    }

    for region, pattern in regions.items():
        clean_file = os.path.join(clean_dir, f"{region}_screen_data.csv")

        if skip is False and os.path.exists(clean_file):
            # Update market prices and market caps
            print(f"Updating market prices and market caps for region {region}")
            try:
                df = pd.read_csv(clean_file)
                if 'Ticker' not in df.columns or 'Market Price' not in df.columns or 'Market Cap' not in df.columns:
                    print(
                        f"Error: Required columns 'Ticker', 'Market Price', or 'Market Cap' not found in {clean_file}")
                    continue
                tickers = df['Ticker'].tolist()
                fetched_data = fetch_stock_data(tickers)
                # Create series for price and market cap
                price_series = pd.Series({k: v['price'] if v is not None else None for k, v in fetched_data.items()})
                market_cap_series = pd.Series(
                    {k: v['market_cap'] if v is not None else None for k, v in fetched_data.items()})
                # Update the DataFrame
                df['Market Price'] = df['Ticker'].map(price_series).combine_first(df['Market Price'])
                df['Market Cap'] = df['Ticker'].map(market_cap_series).combine_first(df['Market Cap'])
                df.to_csv(clean_file, index=False)
                print(f"Updated market prices and market caps for region {region} in {clean_file}")
            except Exception as e:
                print(f"Error updating data for region {region}: {e}")
        else:
            # Full processing
            print(f"Performing full processing for region {region}")
            # Find all CSV files for the region
            files = glob.glob(os.path.join(scraped_dir, pattern))
            if not files:
                print(f"No files found for region {region} in {scraped_dir}")
                continue

            # Extract timestamp from filenames and collect data
            file_data = []
            for file in files:
                match = re.search(r'_(\d{14})\.csv$', file)
                if match:
                    timestamp = match.group(1)  # e.g., 20250418123045
                    try:
                        df = pd.read_csv(file)
                        df['Timestamp'] = timestamp
                        file_data.append(df)
                    except Exception as e:
                        print(f"Error reading {file}: {e}")
                        continue

            if not file_data:
                print(f"No valid data for region {region}")
                continue

            # Combine all data for the region
            combined_df = pd.concat(file_data, ignore_index=True)

            # Ensure required columns exist
            if 'Ticker' not in combined_df.columns or 'Timestamp' not in combined_df.columns:
                print(f"Error: Files for region {region} must contain 'Ticker' and valid timestamp in filename.")
                continue

            # Sort by Ticker and Timestamp (descending) to get the latest entry per ticker
            combined_df['Timestamp'] = pd.to_datetime(combined_df['Timestamp'], format='%Y%m%d%H%M%S')
            combined_df = combined_df.sort_values(by=['Ticker', 'Timestamp'], ascending=[True, False])

            # Keep only the most recent entry per ticker
            clean_df = combined_df.drop_duplicates(subset='Ticker', keep='first')

            # Drop the Timestamp column
            clean_df = clean_df.drop(columns=['Timestamp'])

            # Save to clean data directory
            try:
                clean_df.to_csv(clean_file, index=False)
                print(f"Clean data for region {region} saved to {clean_file}")
            except Exception as e:
                print(f"Error saving clean data for region {region}: {e}")
