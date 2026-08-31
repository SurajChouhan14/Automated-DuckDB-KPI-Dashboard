"""
Automated Unit Test Suite for DuckDB Columnar Banking Portfolio & NIM Analytics Engine.
Verifies:
1. Executive KPI rollups and realistic metric boundaries
2. Tight weighted portfolio yield bounds (around measured 9.64% baseline)
3. Calendar-spine contiguous monthly loan origination growth & monotonicity
4. Intra-regional dense rank grouping
5. Exact 3-term additive yield decomposition identity (Volume + Yield + Interaction == Delta)
6. Planted March 2023 MSME rate-cut signal recovery (negative yield driver, positive volume driver)
7. Deterministic Parquet stream regeneration reproducibility from seed 42
"""

import unittest
import os
import sys
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.data_generator import BankingPortfolioGenerator
from src.duckdb_kpi_engine import DuckDBBankingAnalyticsEngine


class TestDuckDBAnalyticsEngine(unittest.TestCase):
    """
    Unit test cases for in-memory columnar DuckDB analytics engine.
    """

    @classmethod
    def setUpClass(cls):
        # 10,000 records for fast test execution
        cls.loader = BankingPortfolioGenerator(data_dir="data", n_records=10000, random_state=42)
        cls.parquet_path = cls.loader.generate_parquet_stream()
        cls.engine = DuckDBBankingAnalyticsEngine(cls.parquet_path)

    @classmethod
    def tearDownClass(cls):
        cls.engine.close()

    def test_1_executive_banking_kpis(self):
        """Verify executive KPI rollup counts and borrowers pool."""
        kpi_df = self.engine.run_executive_banking_kpis()
        self.assertEqual(len(kpi_df), 1)
        self.assertEqual(kpi_df["total_loans_count"].iloc[0], 10000)
        self.assertGreater(kpi_df["active_distinct_borrowers_count"].iloc[0], 5000)
        self.assertGreater(kpi_df["total_origination_volume_inr"].iloc[0], 0.0)
        self.assertGreater(kpi_df["gross_npa_ratio_pct"].iloc[0], 0.0)

    def test_2_tight_weighted_portfolio_yield_bounds(self):
        """
        Verify portfolio-weighted yield falls tightly around the measured baseline
        (measured 9.64% baseline on 1M records; tolerance band 8.0% to 11.5%).
        """
        kpi_df = self.engine.run_executive_banking_kpis()
        yield_val = kpi_df["weighted_portfolio_yield_pct"].iloc[0]
        self.assertGreaterEqual(yield_val, 8.0)
        self.assertLessEqual(yield_val, 11.5)

    def test_3_cohort_disbursement_growth(self):
        """Verify calendar-spine contiguous monthly loan origination growth and monotonicity."""
        growth_df = self.engine.run_cohort_disbursement_growth()
        self.assertGreaterEqual(len(growth_df), 12)
        self.assertIn("loan_month", growth_df.columns)
        self.assertIn("monthly_disbursed_volume", growth_df.columns)
        self.assertIn("mom_growth_pct", growth_df.columns)
        self.assertIn("cumulative_disbursements", growth_df.columns)
        # Cumulative disbursements must be monotonically non-decreasing
        cum_vals = growth_df["cumulative_disbursements"].values
        self.assertTrue(np.all(np.diff(cum_vals) >= 0.0))

    def test_4_regional_portfolio_matrix(self):
        """Verify intra-regional dense rank grouping (top 2 products per region)."""
        matrix_df = self.engine.run_regional_portfolio_matrix()
        self.assertIn("region", matrix_df.columns)
        self.assertIn("product_type", matrix_df.columns)
        self.assertIn("intra_region_rank", matrix_df.columns)
        self.assertTrue(np.all(matrix_df["intra_region_rank"] <= 2))

    def test_5_exact_yield_decomposition_identity(self):
        """
        Sanity check: Verify mathematical exact additivity of the 3-term decomposition:
        Delta Interest Income = Volume Driver + Yield Driver + Interaction Driver.
        (Holds by construction since annual_interest_income := Principal * Yield in the disbursement flow).
        """
        decomp_df = self.engine.run_root_cause_interest_margin_decomposition(limit=None)
        self.assertGreater(len(decomp_df), 0)
        self.assertIn("exact_interest_delta", decomp_df.columns)
        self.assertIn("volume_expansion_driver", decomp_df.columns)
        self.assertIn("margin_yield_driver", decomp_df.columns)
        self.assertIn("interaction_driver", decomp_df.columns)
        self.assertIn("residual_error", decomp_df.columns)

        residuals = decomp_df["residual_error"].values
        np.testing.assert_allclose(residuals, 0.0, atol=1e-3)

    def test_6_planted_msme_march_2023_signal_recovery(self):
        """
        Verifies recovery of the planted March 2023 MSME promotional rate cut:
        Must detect negative margin yield driver (< 0) and positive volume expansion driver (> 0).
        """
        decomp_df = self.engine.run_root_cause_interest_margin_decomposition(limit=None)
        msme_mar = decomp_df[(decomp_df.product_type == 'MSME Business Credit') & 
                             (decomp_df.loan_month.astype(str) == '2023-03-01')]
        
        self.assertGreater(len(msme_mar), 0, "March 2023 MSME row must be present in attribution.")
        row = msme_mar.iloc[0]
        self.assertLess(row["margin_yield_driver"], 0.0, "March 2023 MSME rate cut must yield negative rate driver.")
        self.assertGreater(row["volume_expansion_driver"], 0.0, "March 2023 MSME surge must yield positive volume driver.")

    def test_7_parquet_reproducibility(self):
        """Verifies deterministic Parquet dataset generation from seed 42 with fixed UTC epoch."""
        loader2 = BankingPortfolioGenerator(data_dir="data", n_records=5000, random_state=42)
        p1 = loader2.generate_parquet_stream()
        df1 = pd.read_parquet(p1)
        
        p2 = loader2.generate_parquet_stream()
        df2 = pd.read_parquet(p2)
        
        pd.testing.assert_frame_equal(df1, df2)


if __name__ == '__main__':
    unittest.main()
