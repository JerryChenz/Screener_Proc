import re
import json
from bs4 import BeautifulSoup
import requests
import pandas as pd


def scrap_data(ticker, url):
    """Scrap data from Yahoo Finance for an input ticker"""

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome'
                      '/71.0.3578.98 Safari/537.36'}
    response = requests.get(url.format(ticker, ticker), headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    pattern = re.compile(r'\s--\sData\s--\s')
    script_data = soup.find('script', text=pattern).contents[0]
    start = script_data.find("context") - 2
    json_data = json.loads(script_data[start:-12])

    return json_data


def _parse_table(json_info, year):
    """Parse the raw list. Return a clean_dict with t-i year and value pair"""
    clean_dict = {}

    for yearly in reversed(json_info):
        if yearly:
            clean_dict[year] = yearly['reportedValue']['raw']
        else:
            clean_dict[year] = 0
        year -= 1

    return clean_dict


def get_income_statement(ticker):
    """Scrape income statement from Yahoo Finance for a given ticker"""

    url_financials = "https://finance.yahoo.com/quote/{}/financials?p={}"
    is_dict = {}

    json_data = scrap_data(ticker, url_financials)
    json_is = json_data['context']['dispatcher']['stores']['QuoteTimeSeriesStore']['timeSeries']

    # last financial year
    last_year = int(list(reversed(json_is['annualTotalRevenue']))[0]['asOfDate'][:4])

    # sales
    sales_dict = _parse_table(json_is['annualTotalRevenue'], last_year)
    is_dict['sales'] = sales_dict
    # cogs
    cogs_dict = {}
    try:
        cogs_dict = _parse_table(json_is['annualCostOfRevenue'], last_year)
        is_dict['cogs'] = cogs_dict
    except KeyError:
        cogs_dict = dict.fromkeys(sales_dict.copy(), 0)
    finally:
        is_dict['cogs'] = cogs_dict
    # operating expenses
    op_cost_dict = {}
    try:
        op_cost_dict = _parse_table(json_is['annualOperatingExpense'], last_year)
    except KeyError:
        op_cost_dict = dict.fromkeys(sales_dict.copy(), 0)
    finally:
        is_dict['op_cost'] = op_cost_dict
    # interest expense
    interest_dict = _parse_table(json_is['annualInterestExpense'], last_year)
    is_dict['interest'] = interest_dict
    # annualNetIncome
    ni_dict = _parse_table(json_is['annualNetIncome'], last_year)
    is_dict['net_income'] = ni_dict

    return is_dict


def get_balance_sheet(ticker):
    """Scrape balance sheet from Yahoo Finance for a given ticker"""

    bs_dict = {}

    # Scrap Data
    url_bs = "https://finance.yahoo.com/quote/{}/balance-sheet?p={}"
    json_data = scrap_data(ticker, url_bs)
    json_bs = json_data['context']['dispatcher']['stores']['QuoteTimeSeriesStore']['timeSeries']

    # last financial year
    last_year = int(list(reversed(json_bs['annualCashAndCashEquivalents']))[0]['asOfDate'][:4])

    # Cash & Cash Equivalents
    cash_dict = _parse_table(json_bs['annualCashAndCashEquivalents'], last_year)
    bs_dict['cash'] = cash_dict
    # short-term Investments
    short_investment_dict = _parse_table(json_bs['annualOtherShortTermInvestments'], last_year)
    bs_dict['short_investment'] = short_investment_dict
    # current assets
    try:
        current_assets_dict = _parse_table(json_bs['annualCurrentAssets'], last_year)
    except KeyError:
        current_assets_dict = dict.fromkeys(cash_dict.copy(), 0)
    finally:
        bs_dict['current_assets'] = current_assets_dict
    # PP&E
    ppe_dict = _parse_table(json_bs['annualNetPPE'], last_year)
    bs_dict['ppe'] = ppe_dict
    # Pre-paid assets
    try:
        prepaid_dict = _parse_table(json_bs['annualNonCurrentPrepaidAssets'], last_year)
    except KeyError:
        prepaid_dict = dict.fromkeys(cash_dict.copy(), 0)
    finally:
        bs_dict['prepaid_assets'] = prepaid_dict
    # Goodwill
    try:
        goodwill_dict = _parse_table(json_bs['annualGoodwillAndOtherIntangibleAssets'], last_year)
    except KeyError:
        goodwill_dict = dict.fromkeys(cash_dict.copy(), 0)
    finally:
        bs_dict['goodwill'] = goodwill_dict
    # current liabilities
    try:
        current_liabilities_dict = _parse_table(json_bs['annualCurrentLiabilities'], last_year)
    except KeyError:
        current_liabilities_dict = dict.fromkeys(cash_dict.copy(), 0)
    finally:
        bs_dict['current_liabilities'] = current_liabilities_dict
    # ST Interest-bearing Debt
    try:
        short_debt_dict = _parse_table(json_bs['annualCurrentDebtAndCapitalLeaseObligation'], last_year)
    except KeyError:
        short_debt_dict = dict.fromkeys(cash_dict.copy(), 0)
    finally:
        bs_dict['short_debt'] = short_debt_dict
    # LT Interest-bearing Debt
    try:
        long_debt_dict = _parse_table(json_bs['annualLongTermDebtAndCapitalLeaseObligation'], last_year)
    except KeyError:
        long_debt_dict = dict.fromkeys(cash_dict.copy(), 0)
    finally:
        bs_dict['long_debt'] = long_debt_dict
    # Total liabilities
    total_liabilities_dict = _parse_table(json_bs['annualTotalLiabilitiesNetMinorityInterest'], last_year)
    bs_dict['total_liabilities'] = total_liabilities_dict

    # Equity
    equity_dict = _parse_table(json_bs['annualTotalEquityGrossMinorityInterest'], last_year)
    bs_dict['equity'] = equity_dict
    # Minority interests
    minority_interest_dict = _parse_table(json_bs['annualMinorityInterest'], last_year)
    bs_dict['minority_interest'] = minority_interest_dict

    return bs_dict


def get_financial_data(ticker):
    """return a nested dictionary containing the income statement and the balance sheet"""

    income_statement = get_income_statement(ticker)
    balance_sheet = get_balance_sheet(ticker)
    fin_data = {ticker: {**income_statement, **balance_sheet}}

    return fin_data


def all_fin_data(ticker_list):
    """return the multi-indexed dataframe containing the financial data of multiple stocks"""

    all_fin_dict = {}
    for ticker in ticker_list:
        print(ticker)
        # add the stock financial data to the main dict
        all_fin_dict[ticker] = get_financial_data(ticker)

    # convert the main to pandas
    fin_data = pd.DataFrame.from_dict({(i, j): all_fin_dict[i][j]
                                       for i in all_fin_dict.keys()
                                       for j in all_fin_dict[i].keys()},
                                      orient='index')

    return fin_data
