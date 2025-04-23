import pandas as pd
import os


def screen_companies():
    """
    Screen companies based on a combined ranking using three financial metrics:
    1. EBIT/Market Cap (higher is better)
    2. ROIC (higher is better)
    3. Composite indicator of Dividend per share/Price and Total Debt/Common Equity

    The composite indicator is formed by summing the ranks of Dividend Yield (higher better)
    and Debt to Equity Ratio (lower better), then ranking that sum. Companies with negative
    or zero common equity are handled separately in the Debt to Equity ranking.
    Results are saved to 'data' directory from cleaned data in 'data/cleaned_data'.
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

        # Identify companies with positive common equity
        df_valid['positive_equity'] = df_valid['Latest Common Equity'] > 0

        # Rank individual metrics
        df_valid['EBIT/Market Cap_rank'] = df_valid['EBIT/Market Cap'].rank(ascending=False, method='min')
        df_valid['ROIC_rank'] = df_valid['ROIC'].rank(ascending=False, method='min')
        df_valid['D/P_rank'] = df_valid['D/P'].rank(ascending=False, method='min')

        # Rank Total Debt/Common Equity, handling negative/zero common equity
        M = df_valid['positive_equity'].sum()  # Number of companies with positive equity
        df_valid.loc[df_valid['positive_equity'], 'Total Debt/Common Equity_rank'] = (
            df_valid.loc[df_valid['positive_equity'], 'Total Debt/Common Equity'].rank(ascending=True, method='min')
        )
        sub_rank = df_valid.loc[~df_valid['positive_equity'], 'Latest Total Debt'].rank(ascending=True, method='min')
        df_valid.loc[~df_valid['positive_equity'], 'Total Debt/Common Equity_rank'] = M + sub_rank

        # Calculate composite indicator
        df_valid['composite_score'] = df_valid['D/P_rank'] + df_valid['Total Debt/Common Equity_rank']
        df_valid['composite_rank'] = df_valid['composite_score'].rank(ascending=True, method='min')

        # Calculate combined rank using the three components
        df_valid['Combined_rank'] = (
            df_valid['EBIT/Market Cap_rank'] +
            df_valid['ROIC_rank'] +
            df_valid['composite_rank']
        )

        # Sort by combined rank (ascending, lower is better)
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
