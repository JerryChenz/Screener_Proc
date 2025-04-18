import pandas as pd
import os
import glob
import re
import yfinance as yf
import time


def fetch_current_prices(tickers, retries=3, wait_time=10):
    """
    Fetch the current market prices for a list of tickers using yfinance.
    Returns a dictionary of ticker to current price, with None for failed fetches.
    """
    prices = {}
    for ticker in tickers:
        for attempt in range(retries):
            try:
                stock = yf.Ticker(ticker)
                prices[ticker] = stock.fast_info['lastPrice']
                break
            except Exception as e:
                print(f"Attempt {attempt + 1} failed for {ticker}: {e}")
                if attempt < retries - 1:
                    time.sleep(wait_time)
                else:
                    print(f"Failed to fetch price for {ticker} after {retries} attempts.")
                    prices[ticker] = None
        time.sleep(0.5)  # Small delay between tickers to avoid API overload
    return prices


def clean_scraped_data(skip=True):
    """
    Consolidate multiple scraped CSV files into three clean regional CSV files.

    Reads all CSV files in `data/scraped_data` with naming patterns `us*_[timestamp].csv`,
    `cn*_[timestamp].csv`, and `hk*_[timestamp].csv`. For each region, combines the data,
    keeping only the most recent entry per ticker based on the timestamp in the filename.
    Saves the consolidated data to `us_screen_data.csv`, `cn_screen_data.csv`, and
    `hk_screen_data.csv` in `data/cleaned_data`.

    If skip is false and the clean file exists, only updates the 'Market Price' column
    efficiently using yfinance, with retries for failed API calls.

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
            # Update only market prices
            print(f"Updating market prices for region {region}")
            try:
                df = pd.read_csv(clean_file)
                if 'Ticker' not in df.columns or 'Market Price' not in df.columns:
                    print(f"Error: Required columns 'Ticker' or 'Market Price' not found in {clean_file}")
                    continue
                tickers = df['Ticker'].tolist()
                updated_prices = fetch_current_prices(tickers)
                successful_prices = {k: v for k, v in updated_prices.items() if v is not None}
                if successful_prices:
                    price_series = pd.Series(successful_prices)
                    df['Market Price'] = df['Ticker'].map(price_series).combine_first(df['Market Price'])
                df.to_csv(clean_file, index=False)
                print(f"Updated market prices for region {region} in {clean_file}")
            except Exception as e:
                print(f"Error updating market prices for region {region}: {e}")
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
