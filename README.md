# Financial Credit Risk ETL & Data Warehousing Pipeline

## 📌 Project Overview
This project is a production-grade ETL (Extract, Transform, Load) pipeline designed to process large-scale financial datasets. It ingests **2.2 million records** of loan data, implements rigorous enterprise data quality checks, and loads the refined data into an **Oracle Star Schema** for risk analytics.

The architecture is specifically designed to mimic enterprise ETL tools like **IBM DataStage** by incorporating "Reject Link" logic for data quality and an "Audit Log" system for job control and monitoring.

---

## 🛠️ Technical Stack
* **Language:** Python 3.12 (Pandas, SQLAlchemy)
* **Database:** Oracle Database XE (SQL, PL/SQL)
* **Orchestration:** Unix/Bash Scripting
* **Connectivity:** `oracledb` (Thin Mode)

---

## 🏗️ Architecture & Features

### 1. High-Volume Processing (Memory Management)
To handle the **2.2M+ record** dataset without memory overflow, the pipeline utilizes a **Chunking Strategy**. Data is processed in batches of **50,000 records**, ensuring low RAM consumption and system stability.

### 2. Data Quality Framework (The "Reject Link")
[cite_start]Following enterprise standards, a validation layer identifies "Bad Data" (e.g., missing or invalid income records)[cite: 1].
* **Valid Data:** Loaded into the Staging environment (`STG_LOAN_DATA`).
* [cite_start]**Rejected Data:** Automatically diverted to an `ERR_LOAN_DATA` table with a specific error reason and timestamp for audit[cite: 1].



### 3. Enterprise Auditing & Job Control
Every execution is tracked in an `ETL_AUDIT_LOG` table, capturing:
* Job Start/End Timestamps
* Final Status (SUCCESS/FAILED)
* Record Counts (Total Processed, Inserted, and Rejected)

---

## 🔄 Workflow
1. **Orchestration:** `run_pipeline.sh` initializes the environment and triggers the Python engine.
2. **Extraction & Cleaning:** `loan_etl.py` reads the CSV, cleans numeric fields (Interest Rates), and validates data.
3. **Oracle Ingestion:** Cleaned data is bulk-inserted into Oracle using explicit **Oracle Dialect Mapping** to ensure decimal precision and avoid "Binary Precision" conflicts.
4. **Warehouse Finalization:** A **PL/SQL Stored Procedure** (`LOAD_FACT_LOANS`) moves data from Staging to the Fact table, generating surrogate keys and final metrics.

---

## 📊 Performance & Results
* **Dataset Size:** 2.2 Million Rows.
* **Batch Speed:** ~1.4 seconds per 50,000 records.
* **Accuracy:** Successfully filtered invalid records (e.g., Annual Income ≤ 0) while maintaining 100% integrity in the `FACT_LOAN_TRANSACTIONS` table.

### **Analytics Summary (Sample Output)**
| Loan Grade | Total Volume | Avg. Loan Amount | Avg. Interest Rate |
| :--- | :--- | :--- | :--- |
| **Grade A** | 86,200 | $14,787.70 | 6.85% |
| **Grade B** | 145,592 | $14,057.80 | 9.96% |
| **Grade G** | 2,290 | $19,614.52 | 27.50% |



---

## 🚀 How to Run
1. Clone the repository.
2. Configure your Oracle connection string in `loan_etl.py`.
3. Execute the setup SQL scripts in the `/SQL` directory to create the Star Schema.
4. Run the pipeline:
   ```bash
   sh run_pipeline.sh
