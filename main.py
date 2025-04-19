import scrape_data
import clean_data
import screen_data

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

if __name__ == "__main__":
    # Step 1: Example list of tickers
    # ticker_list = ['AAPL', 'MSFT', 'GOOGL', 'KHC', 'SIRI']
    # csv_name = 'us'
    # scrape_data.scrape_yfinance(ticker_list, csv_name)

    # Step 2: Clean Data
    clean_data.clean_scraped_data()

    # Step 3: Screen
    screen_data.screen_companies()
