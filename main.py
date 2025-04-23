"""
Financial Metrics:
EBIT/P: Measures earnings yield.
ROIC: Evaluates capital efficiency.
D/P: Represents dividend yield.
Total Debt/P: Assesses debt risk.

Data Filtering:
Exclude companies with missing or invalid data (e.g., zero market price).

Ranking:
Higher values of EBIT/P, ROIC, and D/P are better (ranked in descending order).
Lower values of Total Debt/P are better (ranked in ascending order).

Combined Score:
Sum individual ranks to get a combined score. Lower scores indicate better overall performance.

Output:
Results are saved to data/screened_data with region-specific files (e.g., us_screened.csv).
"""

import scrape_data_yfinance
# import scrape_data_yahooquery
import clean_data
import screen_data
import json


def json_to_list(json_file):
    """
    Read a JSON file and convert its contents into a Python list.

    Args:
        json_file (str): Path to the JSON file.

    Returns:
        list: A list of tickers.

    Raises:
        FileNotFoundError: If the JSON file does not exist.
        json.JSONDecodeError: If the JSON file is invalid or cannot be decoded.
    """

    try:
        with open(json_file, 'r') as file:
            ticker_list = json.load(file)
        return ticker_list

    except FileNotFoundError:
        print(f"Error: The file '{json_file}' does not exist.")
        return []

    except json.JSONDecodeError:
        print(f"Error: The file '{json_file}' contains invalid JSON.")
        return []


if __name__ == "__main__":
    hk_json_path = 'data/ticker_library/hk_unique_tickers.json'
    nasdaq_json_path = 'data/ticker_library/us_nasdaq_tickers.json'
    nyse_json_path = 'data/ticker_library/us_nyse_tickers.json'

    # Step 1: Example list of tickers
    # tickers_list = json_to_list(nyse_json_path)
    # failed_tickers = []
    # csv_name = 'us_nyse'
    # scrape_data_yfinance.scrape_yfinance(tickers_list, csv_name)

    # Step 2: Clean Data
    clean_data.clean_scraped_data()

    # Step 3: Screen
    screen_data.screen_companies()
