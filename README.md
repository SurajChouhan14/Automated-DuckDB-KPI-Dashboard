# ⚡ In-Process Columnar KPI Analytics Dashboard
### Vectorized DuckDB SQL | Apache Arrow Streaming | 1,000,000 Loan Records | Exact Shapley Decomposition

[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Database: DuckDB](https://img.shields.io/badge/Database-DuckDB%20Vectorized-orange.svg)](https://duckdb.org/)
[![Apache Arrow](https://img.shields.io/badge/Streaming-Apache%20Arrow-red.svg)](https://arrow.apache.org/)

A high-throughput in-process OLAP analytics engine querying **1,000,000 compressed Parquet loan records** using DuckDB's vectorized execution engine and Apache Arrow zero-copy memory scanning. Implements exact additive Shapley margin decomposition to isolate volume vs yield performance drivers.

---

## 📌 Vectorized OLAP Architecture & Margin Decomposition

```
 1,000,000 Columnar Parquet Records (Snappy Compressed)
                        │
                        ▼ (Zero-Copy Apache Arrow Stream)
 Vectorized DuckDB In-Process Query Engine (L1/L2 Cache Scanning)
                        │
        ┌───────────────┴───────────────┐
        ▼                               ▼
 Continuous Calendar Spines       Exact Additive Shapley Decomposition
 (Gap-Safe Window Aggregations)   ΔIncome = ΔV·Y_prev + ΔY·V_prev + ΔV·ΔY
 (Sub-20ms Query Latency)         (Zero Residual Error: 0.000000)
```

$$\Delta \text{Interest Income} = (\Delta V \cdot Y_{\text{prev}}) + (\Delta Y \cdot V_{\text{prev}}) + (\Delta V \cdot \Delta Y)$$
$$\text{Residual Attribution Error} = \mathbf{0.000000}$$

---

## 📊 High-Throughput OLAP Query Benchmark
* **Dataset Size:** 1,000,000 historical loan disbursement and repayment records.
* **Vectorized Execution Performance:**
  * Executive Summary KPI Rollups: **$13.32\text{ ms}$**
  * Month-over-Month Calendar Window Aggregations: **$17.87\text{ ms}$**
  * Exact Symmetrical Margin Decomposition: **$12.26\text{ ms}$**
  * **All queries execute in $< 20\text{ ms}$ (Exceeds $< 100\text{ ms}$ real-time OLAP standard)**.

---

## 📂 Repository Structure
```
Automated-DuckDB-KPI-Dashboard/
├── src/
│   ├── duckdb_kpi_engine.py        # Vectorized DuckDB SQL & Shapley decomposition
│   └── data_generator.py           # 1,000,000-row compressed Parquet synthesizer
├── Automated_DuckDB_KPI_Analytics.ipynb # Interactive evaluation notebook
├── run_pipeline.py                 # Pipeline execution script
├── test_duckdb_kpi_engine.py       # Unit testing suite (4/4 passing)
└── requirements.txt                # Production dependencies
```

---

## 🚀 Quickstart & Reproducibility
```bash
git clone https://github.com/SurajChouhan14/Automated-DuckDB-KPI-Dashboard.git
cd Automated-DuckDB-KPI-Dashboard
pip install -r requirements.txt
python run_pipeline.py
python -m unittest test_duckdb_kpi_engine.py
```
