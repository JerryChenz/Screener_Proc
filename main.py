import stock_attributes


if __name__ == '__main__':
    ticker_list = ['1475.HK', 'AAPL']
    for s in ticker_list:
        stock = stock_attributes.Stock(s)
        print(stock.market_cap)
