"""
High-Performance DuckDB Columnar Banking Analytics & Net Interest Margin (NIM) Engine.

Executes sub-second analytical window queries over 1,000,000+ loan records:
1. Executive Banking KPIs (Total Originations, Portfolio Yield, Gross Interest Income, NPA Ratio)
2. Calendar-Spine-Aligned MoM Loan Book Growth Velocity & Cumulative Originations
3. Symmetrically Decomposed Root-Cause Yield Attribution (Volume Growth vs Portfolio Yield/Margin vs Interaction)
4. Regional Lending Portfolio Matrix via DENSE_RANK()
"""

import duckdb
import pandas as pd
import numpy as np


class DuckDBBankingAnalyticsEngine:
    """
    In-memory columnar financial analytical engine powered by DuckDB.
    """

    def __init__(self, parquet_path):
        self.parquet_path = parquet_path.replace("\\", "/")
        self.conn = duckdb.connect(database=':memory:')

    def run_executive_banking_kpis(self):
        """
        Executes core enterprise executive banking KPI rollups across 1M loan disbursements.
        Financial Terminology: Uses 'Total Originations' (not AUM) to accurately reflect disbursement flow.
        """
        query = f"""
        SELECT 
            COUNT(*) AS total_loans_originated,
            COUNT(DISTINCT customer_id) AS total_borrowers,
            ROUND(SUM(principal_disbursed), 2) AS total_originations,
            ROUND(AVG(principal_disbursed), 2) AS average_ticket_size,
            ROUND(SUM(annual_interest_income) / NULLIF(SUM(principal_disbursed), 0) * 100, 2) AS weighted_portfolio_yield_pct,
            ROUND(SUM(annual_interest_income), 2) AS annualized_gross_interest_income,
            ROUND(SUM(npa_flag) * 100.0 / COUNT(*), 2) AS gross_npa_ratio_pct
        FROM read_parquet('{self.parquet_path}');
        """
        return self.conn.execute(query).df()

    def run_cohort_disbursement_growth(self):
        """
        Executes monthly loan book growth velocity and cumulative originations using a continuous Calendar Spine.
        Guarantees gap-safe LAG() window evaluation across 18 consecutive months with clean cold-start initial growth handling.
        """
        query = f"""
        WITH calendar_spine AS (
            SELECT CAST(range AS DATE) AS month_start
            FROM range(DATE '2023-01-01', DATE '2024-07-01', INTERVAL 1 MONTH)
        ),
        monthly_origination AS (
            SELECT 
                DATE_TRUNC('month', disbursement_date)::DATE AS loan_month,
                SUM(principal_disbursed) AS monthly_disbursement,
                SUM(annual_interest_income) AS monthly_projected_interest,
                COUNT(DISTINCT customer_id) AS active_borrowers,
                COUNT(*) AS loan_count
            FROM read_parquet('{self.parquet_path}')
            GROUP BY 1
        ),
        continuous_disbursements AS (
            SELECT 
                c.month_start AS loan_month,
                ROUND(COALESCE(m.monthly_disbursement, 0.0), 2) AS monthly_disbursed_volume,
                ROUND(COALESCE(m.monthly_projected_interest, 0.0), 2) AS monthly_interest_revenue,
                COALESCE(m.active_borrowers, 0) AS active_borrowers,
                COALESCE(m.loan_count, 0) AS loan_count
            FROM calendar_spine c
            LEFT JOIN monthly_origination m ON c.month_start = m.loan_month
        ),
        growth_calculations AS (
            SELECT 
                loan_month,
                monthly_disbursed_volume,
                active_borrowers,
                loan_count,
                LAG(monthly_disbursed_volume) OVER (ORDER BY loan_month) AS prev_month_disbursement,
                -- Clean Cold-Start: Handle Month 1 baseline (0.00% delta)
                COALESCE(ROUND((monthly_disbursed_volume - LAG(monthly_disbursed_volume) OVER (ORDER BY loan_month)) / 
                               NULLIF(LAG(monthly_disbursed_volume) OVER (ORDER BY loan_month), 0) * 100, 2), 0.00) AS mom_growth_pct,
                ROUND(SUM(monthly_disbursed_volume) OVER (ORDER BY loan_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS cumulative_disbursements
            FROM continuous_disbursements
        )
        SELECT * FROM growth_calculations ORDER BY loan_month;
        """
        return self.conn.execute(query).df()

    def run_regional_portfolio_matrix(self):
        """
        Executes multi-dimensional cross-tabulation across Region x Product Type with intra-regional DENSE_RANK().
        """
        query = f"""
        WITH regional_agg AS (
            SELECT 
                region,
                product_type,
                ROUND(SUM(principal_disbursed), 2) AS product_disbursed_volume,
                ROUND(AVG(portfolio_yield_pct), 2) AS avg_product_yield_pct,
                COUNT(*) AS loan_count
            FROM read_parquet('{self.parquet_path}')
            GROUP BY region, product_type
        ),
        ranked_portfolio AS (
            SELECT 
                region,
                product_type,
                product_disbursed_volume,
                avg_product_yield_pct,
                loan_count,
                ROUND(product_disbursed_volume / SUM(product_disbursed_volume) OVER () * 100, 2) AS share_of_national_book_pct,
                DENSE_RANK() OVER (PARTITION BY region ORDER BY product_disbursed_volume DESC) AS intra_region_rank
            FROM regional_agg
        )
        SELECT * FROM ranked_portfolio WHERE intra_region_rank <= 2 ORDER BY region, intra_region_rank;
        """
        return self.conn.execute(query).df()

    def run_root_cause_interest_margin_decomposition(self):
        """
        Exact Symmetrical Root-Cause Interest Income Decomposition:
        Recovers designed economic signals (e.g. March 2023 MSME Yield Cut vs Volume Surge).
        
        Decomposition Identity:
        Delta Interest Income = (Delta Volume * Yield_prev) + (Delta Yield * Volume_prev) + (Delta Volume * Delta Yield)
        Guarantees: Volume Driver + Yield Margin Driver + Interaction = Exact Delta Interest Income (Residual Error = 0.0000).
        """
        query = f"""
        WITH monthly_product_yield AS (
            SELECT 
                DATE_TRUNC('month', disbursement_date)::DATE AS loan_month,
                product_type,
                SUM(principal_disbursed) AS total_volume,
                SUM(annual_interest_income) AS interest_income,
                SUM(annual_interest_income) / NULLIF(SUM(principal_disbursed), 0) AS effective_yield_rate
            FROM read_parquet('{self.parquet_path}')
            GROUP BY 1, 2
        ),
        delta_computation AS (
            SELECT 
                loan_month,
                product_type,
                total_volume,
                interest_income,
                effective_yield_rate,
                LAG(total_volume) OVER (PARTITION BY product_type ORDER BY loan_month) AS prev_volume,
                LAG(effective_yield_rate) OVER (PARTITION BY product_type ORDER BY loan_month) AS prev_yield,
                LAG(interest_income) OVER (PARTITION BY product_type ORDER BY loan_month) AS prev_interest_income
            FROM monthly_product_yield
        ),
        exact_attribution AS (
            SELECT 
                loan_month,
                product_type,
                ROUND(interest_income - prev_interest_income, 2) AS exact_interest_delta,
                -- 1. Pure Volume Expansion Driver: Delta V * Yield_prev
                ROUND((total_volume - prev_volume) * prev_yield, 2) AS volume_expansion_driver,
                -- 2. Pure Yield/Rate Margin Driver: Delta Yield * V_prev
                ROUND(prev_volume * (effective_yield_rate - prev_yield), 2) AS margin_yield_driver,
                -- 3. Exact Cross-Product Interaction: Delta V * Delta Yield
                ROUND((total_volume - prev_volume) * (effective_yield_rate - prev_yield), 2) AS interaction_driver,
                -- Verify Mathematical Additivity
                ROUND(((total_volume - prev_volume) * prev_yield + 
                       prev_volume * (effective_yield_rate - prev_yield) + 
                       (total_volume - prev_volume) * (effective_yield_rate - prev_yield)) - (interest_income - prev_interest_income), 4) AS residual_error
            FROM delta_computation
            WHERE prev_interest_income IS NOT NULL
        )
        SELECT 
            loan_month,
            product_type,
            exact_interest_delta,
            volume_expansion_driver,
            margin_yield_driver,
            interaction_driver,
            residual_error
        FROM exact_attribution
        ORDER BY ABS(exact_interest_delta) DESC
        LIMIT 5;
        """
        return self.conn.execute(query).df()
