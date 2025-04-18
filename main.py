import scrap_data

if __name__ == "__main__":
    # Example list of tickers
    ticker_list = ['AAPL', 'MSFT', 'GOOGL']
    csv_name = 'US_stock_test'
    scrap_data.scrap_yfinance(ticker_list, csv_name)
