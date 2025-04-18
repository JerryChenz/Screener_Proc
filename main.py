import scrap_data
import clean_data

if __name__ == "__main__":
    # Example list of tickers
    ticker_list = ['AAPL', 'MSFT', 'GOOGL', 'KHC', 'SIRI']
    csv_name = 'US_'
    scrap_data.scrap_yfinance(ticker_list, csv_name)
    clean_data.clean_scraped_data()
