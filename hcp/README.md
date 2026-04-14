# 🏥 Healthcare Claims Analytics Pipeline

> **An end-to-end ETL pipeline that ingests multi-source healthcare claims data, applies cleaning and validation logic, computes business-critical KPIs, and delivers insights via interactive Power BI / Tableau dashboards.**

<p>
  <img src="https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/SQL-4479A1?style=flat&logo=postgresql&logoColor=white" />
  <img src="https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white" />
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white" />
  <img src="https://img.shields.io/badge/AWS%20S3-232F3E?style=flat&logo=amazons3&logoColor=white" />
  <img src="https://img.shields.io/badge/Azure%20Blob-0078D4?style=flat&logo=microsoftazure&logoColor=white" />
  <img src="https://img.shields.io/badge/Power%20BI-F2C811?style=flat&logo=powerbi&logoColor=black" />
  <img src="https://img.shields.io/badge/Tableau-E97627?style=flat&logo=tableau&logoColor=white" />
</p>

---

## 📌 Overview

Healthcare organisations process millions of claims records across disparate systems — SQL databases, cloud storage, APIs, and flat files. Turning this raw data into consistent, reliable KPIs is slow, manual, and error-prone.

This project solves that problem with a fully automated pipeline that:

- **Extracts** claims data from SQL databases, AWS S3, Azure Blob Storage, APIs, and CSV/Excel files
- **Transforms** raw records using validated cleaning logic, deduplication, and business rules
- **Computes** 7 categories of healthcare KPIs — approval rates, processing time, cost variance, denial trends, and more
- **Loads** results into a data warehouse and exports dashboard-ready CSVs
- **Visualises** KPIs via Power BI and Tableau dashboards

---

## 🏗️ Pipeline Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                             │
│  SQL Database │ AWS S3 │ Azure Blob │ REST API │ CSV / Excel     │
└──────────────────────────┬───────────────────────────────────────┘
                           │
                           ▼
           ┌───────────────────────────┐
           │       EXTRACT LAYER       │
           │  extract_sql.py           │
           │  extract_s3.py            │
           │  extract_csv.py           │
           └──────────────┬────────────┘
                          │
                          ▼
           ┌───────────────────────────┐
           │     TRANSFORM LAYER       │
           │  clean.py                 │
           │  · Type casting           │
           │  · Null handling          │
           │  · Deduplication          │
           │  · Value normalisation    │
           │                           │
           │  kpi_engine.py            │
           │  · Approval / denial rate │
           │  · Processing time        │
           │  · Cost variance          │
           │  · Denial by reason       │
           │  · Monthly volume         │
           │  · By region / plan type  │
           └──────────────┬────────────┘
                          │
                          ▼
           ┌───────────────────────────┐
           │       LOAD LAYER          │
           │  load_warehouse.py        │
           │  · SQL warehouse tables   │
           │  · Parquet export         │
           │  · CSV export             │
           └──────────────┬────────────┘
                          │
                          ▼
           ┌───────────────────────────┐
           │         OUTPUTS           │
           │  Power BI Dashboard       │
           │  Tableau Dashboard        │
           │  SQL KPI Reports          │
           │  Jupyter Analysis Notebooks│
           └───────────────────────────┘
```

---

## ✨ Key Features

- **Multi-source ingestion** — unified connectors for SQL, AWS S3, Azure Blob, REST API, and flat files
- **Production-grade cleaning** — type casting, null imputation, deduplication, and value normalisation at each stage
- **KPI computation engine** — 7 KPI categories covering approval rates, processing times, cost analysis, and denial trends
- **Data quality validation** — schema checks, range validation, and null audits with detailed logging
- **Dashboard-ready output** — pre-aggregated CSVs and warehouse tables optimised for Power BI and Tableau
- **Reusable & modular** — each pipeline stage is independently importable and testable
- **Sample data generator** — 500-record realistic dataset for testing without needing live data

---

## 🗂️ Project Structure

```
healthcare-claims-pipeline/
│
├── 📁 pipelines/
│   ├── extract/
│   │   ├── extract_csv.py          # CSV / Excel ingestion
│   │   ├── extract_sql.py          # SQL database connector (SQLAlchemy)
│   │   └── extract_s3.py           # AWS S3 ingestion (boto3)
│   ├── transform/
│   │   ├── clean.py                # Full cleaning & validation pipeline
│   │   └── kpi_engine.py           # KPI calculation engine (7 KPI categories)
│   └── load/
│       └── load_warehouse.py       # Load to SQL warehouse + CSV/Parquet export
│
├── 📁 sql/
│   ├── kpis/
│   │   └── kpi_queries.sql         # 8 core KPI SQL queries
│   ├── validation/
│   │   └── data_checks.sql         # Data quality check queries
│   └── reports/
│       └── monthly_report.sql      # Monthly executive report query
│
├── 📁 notebooks/
│   ├── 01_data_exploration.ipynb   # Source data profiling & EDA
│   ├── 02_pipeline_validation.ipynb # Pipeline QA and output checks
│   └── 03_kpi_analysis.ipynb       # KPI trend analysis & visualisation
│
├── 📁 dashboards/
│   ├── powerbi/                    # Power BI (.pbix) dashboard files
│   └── tableau/                    # Tableau (.twbx) dashboard files
│
├── 📁 data/
│   ├── raw/                        # Raw source files (gitignored)
│   ├── processed/                  # Pipeline output files (gitignored)
│   └── sample/
│       ├── claims_sample.csv       # 500-row sample dataset
│       └── generate_sample_data.py # Script to regenerate sample data
│
├── 📁 docs/
│   └── data_dictionary.md          # Field definitions and KPI logic
│
├── 📁 config/
│   └── config.yaml                 # Connection config template
│
├── 📁 tests/
│   └── test_clean.py               # Unit tests for cleaning logic
│
├── .gitignore
├── requirements.txt
└── README.md
```

---

## 📊 KPIs Tracked

| # | KPI Category | Metrics |
|---|---|---|
| 1 | Approval & Denial Rate | Overall approval %, denial %, pending % |
| 2 | Processing Time | Avg, median, max days; % processed < 5 days |
| 3 | Cost Summary | Total billed, paid, allowed; cost variance; payment ratio |
| 4 | Denial by Reason | Count and % breakdown by denial reason code |
| 5 | Monthly Volume | Claim volume, approval rate, avg cost per month |
| 6 | KPIs by Region | Approval rate, total billed, avg processing time per region |
| 7 | KPIs by Plan Type | Approval/denial rates and avg paid per plan (PPO, HMO, EPO, HDHP) |

---

## ⚙️ Setup & Installation

### Prerequisites

- Python 3.8+
- Access to at least one data source (or use the sample data generator)
- Power BI Desktop or Tableau Desktop for dashboards (optional)

### 1. Clone the repository

```bash
git clone https://github.com/sushmitha-kokkalgave/healthcare-claims-pipeline.git
cd healthcare-claims-pipeline
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure connections

```bash
cp config/config.yaml config/config_local.yaml
# Edit config_local.yaml with your credentials
# Never commit real credentials to Git
```

### 4. Generate sample data (no live system needed)

```bash
python data/sample/generate_sample_data.py
```

### 5. Run the full pipeline

```bash
# Extract
python pipelines/extract/extract_csv.py

# Transform + KPI computation
python pipelines/transform/clean.py
python pipelines/transform/kpi_engine.py

# Load
python pipelines/load/load_warehouse.py
```

### 6. Explore in Jupyter

```bash
jupyter notebook notebooks/01_data_exploration.ipynb
```

---

## 📈 Sample Output

After running the pipeline on the sample dataset you can expect results like:

| KPI | Value |
|-----|-------|
| Total Claims | 500 |
| Approval Rate | ~70% |
| Avg Processing Time | ~10 days |
| Total Billed Amount | ~$1.2M |
| Top Denial Reason | "not covered" |
| Highest Volume Region | NE |

---

## 🧪 Running Tests

```bash
pytest tests/ -v
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|------------|
| Language | Python 3.8+, SQL |
| Big Data | PySpark, Databricks |
| Cloud | AWS S3 (boto3), Azure Blob Storage |
| Databases | PostgreSQL, Snowflake, SQL Server |
| Visualization | Power BI (DAX), Tableau |
| Libraries | Pandas, NumPy, SQLAlchemy, PyArrow |
| Testing | Pytest |
| Config | YAML, python-dotenv |

---

## 🤝 Contributing

Contributions, issues, and feature requests are welcome! Feel free to open an issue or submit a pull request.

---

## 👩‍💻 Author

**Sushmitha Kokkalgave** — Senior Data Analyst  
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=flat&logo=linkedin&logoColor=white)](https://linkedin.com/in/sushmitha-kokkalgave)
[![Email](https://img.shields.io/badge/Email-EA4335?style=flat&logo=gmail&logoColor=white)](mailto:susmitha.data97@gmail.com)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=flat&logo=github&logoColor=white)](https://github.com/sushmitha-kokkalgave)

---

<p align="center"><i>Built with Python · Powered by real healthcare data patterns · Driven by KPIs that matter.</i></p>
