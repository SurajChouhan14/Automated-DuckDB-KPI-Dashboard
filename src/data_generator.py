"""
Banking Credit Portfolio & Loan Disbursement Data Generation Engine.

Generates 1,000,000 corporate and retail loan disbursement & transaction records
with fixed UTC epoch timestamps (timezone-immune) and designed macro-economic signals:
- Ground-Truth March 2023 Promotional Credit Rate Cut (-150 bps yield cut, +40% volume surge)
- Ground-Truth October 2023 Festive Retail Lending Push (-50 bps yield cut, +25% volume expansion)
"""

import os
import numpy as np
import pandas as pd

try:
    import duckdb
    _DUCKDB_AVAILABLE = True
except ImportError:
    _DUCKDB_AVAILABLE = False


class BankingPortfolioGenerator:
    """
    Generates realistic commercial banking loan disbursements with designed macro signals.
    """

    def __init__(self, data_dir="data", n_records=1000000, random_state=42):
        self.data_dir = data_dir
        self.n_records = n_records
        self.random_state = random_state
        os.makedirs(self.data_dir, exist_ok=True)
        self.parquet_path = os.path.join(self.data_dir, "banking_loans_stream.parquet")

    def generate_parquet_stream(self):
        """
        Generates 1,000,000 banking loan disbursements across:
        - Products: Home Loans, MSME Business Credit, Auto Loans, Personal Loans, Corporate Working Capital
        - Regions: North Zone, West Zone, South Zone, East Zone
        - Channels: Digital Banking, Branch Network, DSA Partner
        
        Borrower Structure:
        - Borrowers are drawn from a fixed pool of 250,000; COUNT(DISTINCT) reflects active in-window
          borrowers (~245k), yielding ~4 facilities per active borrower over 18 months.
        
        Income Modeling:
        - Income is modeled as first-year projected interest on disbursed principal (simple, non-amortized),
          appropriate for origination-flow attribution.
        """
        if os.path.exists(self.parquet_path):
            try:
                os.remove(self.parquet_path)
            except Exception:
                pass

        np.random.seed(self.random_state)
        n = self.n_records

        # Intentional customer pool: 250,000 unique borrowers for repeat credit facilities
        n_unique_borrowers = 250000
        customer_pool = np.arange(100001, 100001 + n_unique_borrowers)
        customer_ids = np.random.choice(customer_pool, size=n, replace=True)
        
        products = ['Home Loans', 'MSME Business Credit', 'Auto Loans', 'Personal Loans', 'Corporate Working Capital']
        product_probs = [0.35, 0.25, 0.20, 0.12, 0.08]
        product_col = np.random.choice(products, n, p=product_probs)
        
        regions = ['West Zone (Mumbai)', 'North Zone (Delhi-NCR)', 'South Zone (Bangalore-Chennai)', 'East Zone (Kolkata)']
        region_col = np.random.choice(regions, n, p=[0.38, 0.28, 0.24, 0.10])
        
        channels = ['Digital Banking', 'Branch Network', 'DSA Partner']
        channel_col = np.random.choice(channels, n, p=[0.55, 0.35, 0.10])

        # Fixed UTC epoch conversion (immune to local machine timezone drift)
        start_ts = int(pd.Timestamp("2023-01-01").value // 10**9)
        end_ts = int(pd.Timestamp("2024-06-30").value // 10**9)
        random_ts = np.random.randint(start_ts, end_ts, n)
        timestamps = pd.to_datetime(random_ts, unit='s')
        months = timestamps.strftime('%Y-%m')

        # Base Principal Amounts (Disbursements)
        base_principals = {
            'Home Loans': 4500000.0,
            'MSME Business Credit': 2200000.0,
            'Corporate Working Capital': 12000000.0,
            'Auto Loans': 850000.0,
            'Personal Loans': 350000.0
        }
        
        # Base Annualized Portfolio Yield / Interest Rates (%)
        base_yields = {
            'Home Loans': 8.50,
            'MSME Business Credit': 12.25,
            'Corporate Working Capital': 9.75,
            'Auto Loans': 10.50,
            'Personal Loans': 15.00
        }

        principals = np.array([base_principals[p] * np.random.uniform(0.7, 1.4) for p in product_col]).round(2)
        yields = np.array([base_yields[p] + np.random.normal(0, 0.35) for p in product_col]).round(2)
        tenure_months = np.random.choice([12, 36, 60, 120, 240], n, p=[0.15, 0.25, 0.25, 0.20, 0.15])

        # -------------------------------------------------------------
        # INJECT GROUND-TRUTH ECONOMIC SIGNALS:
        # 1. March 2023 MSME Credit Campaign:
        #    - 150 bps promotional rate cut + 40% volume surge
        # -------------------------------------------------------------
        msme_mar23_mask = (product_col == 'MSME Business Credit') & (months == '2023-03')
        yields[msme_mar23_mask] = (yields[msme_mar23_mask] - 1.50).clip(7.0, 20.0)
        principals[msme_mar23_mask] = principals[msme_mar23_mask] * 1.40

        # 2. October 2023 Festive Home Loan Push:
        #    - 50 bps festive rate discount + 25% volume expansion
        home_oct23_mask = (product_col == 'Home Loans') & (months == '2023-10')
        yields[home_oct23_mask] = (yields[home_oct23_mask] - 0.50).clip(6.5, 18.0)
        principals[home_oct23_mask] = principals[home_oct23_mask] * 1.25

        # Monthly Projected Interest Income (Annualized Yield * Principal / 12)
        annual_interest_income = (principals * (yields / 100.0)).round(2)
        monthly_interest_income = (annual_interest_income / 12.0).round(2)

        # Risk Classification (NPA Indicator)
        npa_flag = np.where(np.random.uniform(0, 1, n) < np.where(product_col == 'Personal Loans', 0.035, 0.012), 1, 0)

        df = pd.DataFrame({
            'disbursement_id': np.arange(1, n + 1),
            'customer_id': customer_ids,
            'product_type': product_col,
            'region': region_col,
            'channel': channel_col,
            'disbursement_date': timestamps,
            'principal_disbursed': principals,
            'portfolio_yield_pct': yields,
            'tenure_months': tenure_months,
            'annual_interest_income': annual_interest_income,
            'monthly_interest_income': monthly_interest_income,
            'npa_flag': npa_flag
        })

        try:
            df.to_parquet(self.parquet_path, index=False, engine='pyarrow', compression='snappy')
        except Exception:
            if _DUCKDB_AVAILABLE:
                con = duckdb.connect()
                escaped_path = self.parquet_path.replace("\\", "/")
                con.register("df_to_save", df)
                con.execute(f"COPY df_to_save TO '{escaped_path}' (FORMAT PARQUET, CODEC 'SNAPPY')")
                con.close()
            else:
                df.to_parquet(self.parquet_path, index=False)

        return self.parquet_path
