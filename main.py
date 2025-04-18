import scrape_data
import clean_data

if __name__ == "__main__":
    # Step 1: Example list of tickers
    ticker_list = ['AAPL', 'MSFT', 'GOOGL', 'KHC', 'SIRI']
    csv_name = 'us'
    scrape_data.scrape_yfinance(ticker_list, csv_name)

    # Step 2: Clean Data
    clean_data.clean_scraped_data()

    # Step 3: Screen
