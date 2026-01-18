# Financial Credit Risk ETL & Data Warehousing Pipeline
Project Overview
This project demonstrates a production-grade ETL (Extract, Transform, Load) pipeline designed to process large-scale financial data. The system ingests 2.2 million records of loan data, performs rigorous data quality checks, and loads the refined data into an Oracle Star Schema for risk analytics.

The architecture mimics enterprise tools like IBM DataStage by implementing a "Reject Link" logic for data quality and an "Audit Log" for job control.

Technical Stack
Language: Python 3.12 (Pandas, SQLAlchemy)

Database: Oracle Database XE (SQL, PL/SQL)

Orchestration: Unix/Bash Scripting

Driver: oracledb (Thin Mode)

Key Architecture Features
1. High-Volume Processing (Memory Management)
To handle the 2.2M+ record dataset without memory overflow, the pipeline utilizes a Chunking Strategy. Data is processed in batches of 50,000 records, ensuring low RAM consumption and high stability.

2. Data Quality Framework (The "Reject Link")
Following enterprise standards, a validation layer identifies "Bad Data" (e.g., missing or invalid income records).

Valid Data: Loaded into the Staging environment.

Rejected Data: Automatically diverted to an ERR_LOAN_DATA table with a specific error reason and timestamp for audit.

3. Enterprise Auditing & Job Control
Every execution is tracked in an ETL_AUDIT_LOG table, capturing:

Job Start/End Timestamps

Final Status (SUCCESS/FAILED)

Record Counts (Total Processed, Inserted, and Rejected)

Workflow
Orchestration: run_pipeline.sh initializes the environment and triggers the Python engine.

Extraction & Cleaning: loan_etl.py reads the CSV, cleans numeric fields (Interest Rates), and validates data.

Oracle Ingestion: Cleaned data is bulk-inserted into STG_LOAN_DATA using explicit Oracle Dialect Mapping to ensure decimal precision.

Transformation: A PL/SQL Stored Procedure (LOAD_FACT_LOANS) moves data from Staging to the Fact table, generating surrogate keys.

Performance & Results
Dataset Size: 2.2 Million Rows.

Batch Speed: ~1.4 seconds per 50,000 records.

Accuracy: Successfully filtered out invalid records (e.g., Annual Income ≤ 0) while maintaining 100% integrity in the FACT_LOAN_TRANSACTIONS table.
