"""
Automated Unit Test Suite for DuckDB Columnar Banking Portfolio & NIM Analytics Engine.
Verifies Parquet Ingestion, Executive KPI Queries, Calendar-Spine Growth, and Exact Yield Decomposition.
"""

import unittest
import os
import sys
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.data_loader import BankingPortfolioGenerator
from src.duckdb_analytics_engine import DuckDBBankingAnalyticsEngine


class TestDuckDBAnalyticsEngine(unittest.TestCase):
    """
    Unit test cases for in-memory columnar DuckDB analytics engine.
    """

    @classmethod
    def setUpClass(cls):
        # Generate smaller test parquet dataset (10,000 rows for fast testing)
        cls.loader = BankingPortfolioGenerator(data_dir="data", n_records=10000, random_state=42)
        cls.parquet_path = cls.loader.generate_parquet_stream()
        cls.engine = DuckDBBankingAnalyticsEngine(cls.parquet_path)

    def test_executive_banking_kpis(self):
        """Verify executive KPI rollup metrics."""
        kpi_df = self.engine.run_executive_banking_kpis()
        self.assertEqual(len(kpi_df), 1)
        self.assertEqual(kpi_df["total_loans_originated"].iloc[0], 10000)
        self.assertGreater(kpi_df["total_originations"].iloc[0], 0.0)
        self.assertGreater(kpi_df["weighted_portfolio_yield_pct"].iloc[0], 5.0)
        self.assertLess(kpi_df["weighted_portfolio_yield_pct"].iloc[0], 25.0)
        self.assertGreater(kpi_df["gross_npa_ratio_pct"].iloc[0], 0.0)

    def test_cohort_disbursement_growth(self):
        """Verify calendar-spine contiguous monthly loan origination growth."""
        growth_df = self.engine.run_cohort_disbursement_growth()
        self.assertGreaterEqual(len(growth_df), 12)
        self.assertIn("loan_month", growth_df.columns)
        self.assertIn("monthly_disbursed_volume", growth_df.columns)
        self.assertIn("mom_growth_pct", growth_df.columns)
        self.assertIn("cumulative_disbursements", growth_df.columns)
        # Cumulative disbursements must be monotonically non-decreasing
        cum_vals = growth_df["cumulative_disbursements"].values
        self.assertTrue(np.all(np.diff(cum_vals) >= 0.0))

    def test_regional_portfolio_matrix(self):
        """Verify intra-regional dense rank grouping."""
        matrix_df = self.engine.run_regional_portfolio_matrix()
        self.assertIn("region", matrix_df.columns)
        self.assertIn("product_type", matrix_df.columns)
        self.assertIn("intra_region_rank", matrix_df.columns)
        # Filtered to top 2 products per region
        self.assertTrue(np.all(matrix_df["intra_region_rank"] <= 2))

    def test_exact_yield_decomposition_identity(self):
        """Verify mathematical exact additivity (residual error == 0.0000)."""
        decomp_df = self.engine.run_root_cause_interest_margin_decomposition()
        self.assertGreater(len(decomp_df), 0)
        self.assertIn("exact_interest_delta", decomp_df.columns)
        self.assertIn("volume_expansion_driver", decomp_df.columns)
        self.assertIn("margin_yield_driver", decomp_df.columns)
        self.assertIn("interaction_driver", decomp_df.columns)
        self.assertIn("residual_error", decomp_df.columns)

        # Exact additivity check: Volume Driver + Yield Driver + Interaction == Delta
        residuals = decomp_df["residual_error"].values
        np.testing.assert_allclose(residuals, 0.0, atol=1e-3)


if __name__ == '__main__':
    unittest.main()
