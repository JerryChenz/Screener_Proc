import pandas as pd
import os


def screen_companies():
    """
    Screen companies based on the combined ranking of four financial metrics:
    1. EBIT/P (Earnings Yield)
    2. ROIC (Return on Invested Capital)
    3. D/P (Dividend Yield)
    4. Total Debt/Total Assets (Debt to Total Assets Ratio)

    The function reads cleaned data from 'data/cleaned_data' and saves the screened results to 'data'.
    """
    # Define directories
    cleaned_dir = os.path.join(os.getcwd(), 'data', 'cleaned_data')
    screened_dir = os.path.join(os.getcwd(), 'data')
    os.makedirs(screened_dir, exist_ok=True)

    regions = ['us', 'cn', 'hk']  # Supported regions

    for region in regions:
        input_path = os.path.join(cleaned_dir, f"{region}_screen_data.csv")
        output_path = os.path.join(screened_dir, f"{region}_screened.csv")

        if not os.path.exists(input_path):
            print(f"Cleaned data not found for {region}. Skipping...")
            continue

        # Load cleaned data
        df = pd.read_csv(input_path)

        # Calculate EBIT: Sales - COGS - Operating Expenses
        df['EBIT'] = (
            df['Past Annual Sales']
            - df['Past Annual Cogs']
            - df['Past Annual Opex']
        )

        # Calculate financial metrics
        df['EBIT/Market Cap'] = df['EBIT'] / df['Market Cap']
        df['ROIC'] = df['EBIT'] / df['Latest Invested Capital']
        df['D/P'] = df['Past Financial Year Dividends'] / df['Market Price']
        df['Total Debt/Common Equity'] = df['Latest Total Debt'] / df['Latest Common Equity']

        # Filter valid rows (non-negative and non-zero required fields)
        valid_mask = (
            (df['Market Price'] > 0) &
            (df['Market Cap'] > 0) &
            (df['Latest Invested Capital'] > 0) &
            (df['EBIT'].notna()) &
            (df['Past Financial Year Dividends'].notna()) &
            (df['Latest Common Equity'].notna()) &
            (df['Latest Total Debt'].notna())
        )
        df_valid = df[valid_mask].copy()

        if df_valid.empty:
            print(f"No valid data remaining for {region} after filtering.")
            continue

        # Rank each metric
        df_valid['EBIT/Market Cap_rank'] = df_valid['EBIT/Market Cap'].rank(ascending=False, method='min')
        df_valid['ROIC_rank'] = df_valid['ROIC'].rank(ascending=False, method='min')
        df_valid['D/P_rank'] = df_valid['D/P'].rank(ascending=False, method='min')
        df_valid['Total Debt/Common Equity_rank'] = df_valid['Total Debt/Common Equity'].rank(ascending=True, method='min')

        # Calculate combined rank
        df_valid['Combined_rank'] = df_valid[
            ['EBIT/Market Cap_rank', 'ROIC_rank', 'D/P_rank', 'Total Debt/Common Equity_rank']
        ].sum(axis=1)

        # Sort by combined rank (ascending)
        df_sorted = df_valid.sort_values('Combined_rank')

        # Select relevant columns for output
        output_columns = [
            'Ticker', 'Company Name', 'Industry', 'Market Price', 'Market Cap', 'Market Currency',
            'EBIT/Market Cap', 'ROIC', 'D/P', 'Total Debt/Common Equity', 'Combined_rank'
        ]
        df_output = df_sorted[output_columns]

        # Save to CSV
        df_output.to_csv(output_path, index=False)
        print(f"Screened data for {region} saved to {output_path}")
