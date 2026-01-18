import pandas as pd
import sqlalchemy
from sqlalchemy.dialects import oracle
import oracledb
import time
import sys
from datetime import datetime

# --- STEP 1: COMPATIBILITY ---
oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb

# --- STEP 2: CONFIGURATION ---
DB_CONN = "oracle+cx_oracle://system:root@localhost:1521/?service_name=XE"
FILE_PATH = r'F:\JOB HUNT\Projects\Financial Credit Risk ETL Pipeline\data\raw\accepted_2007_to_2018Q4.csv'

# Define the exact Oracle types for every column to prevent "Binary Precision" errors
ORACLE_TYPES = {
    'id': oracle.VARCHAR2(50),
    'loan_amnt': oracle.NUMBER(15, 2),
    'term': oracle.VARCHAR2(20),
    'int_rate': oracle.NUMBER(10, 2),
    'grade': oracle.VARCHAR2(5),
    'emp_length': oracle.VARCHAR2(50),
    'annual_inc': oracle.NUMBER(15, 2),
    'loan_status': oracle.VARCHAR2(50),
    'addr_state': oracle.VARCHAR2(5)
}

def process_etl():
    engine = sqlalchemy.create_engine(DB_CONN)
    start_ts = datetime.now()
    total_good, total_bad = 0, 0
    
    print(f"--- Pipeline Started at {start_ts} ---")

    # A. INITIAL AUDIT LOG
    try:
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text(
                'INSERT INTO "ETL_AUDIT_LOG" ("JOB_NAME", "START_TIME", "STATUS") VALUES (:name, :ts, :stat)'),
                {"name": "FIN_LOAN_ETL", "ts": start_ts, "stat": "RUNNING"}
            )
            conn.commit()
    except Exception as e: print(f"Audit Log Note: {e}")

    try:
        cols = ['id', 'loan_amnt', 'term', 'int_rate', 'grade', 'emp_length', 'annual_inc', 'loan_status', 'addr_state']

        for chunk in pd.read_csv(FILE_PATH, chunksize=50000, usecols=cols, low_memory=False):
            # 1. Clean Interest Rate
            if chunk['int_rate'].dtype == 'object':
                chunk['int_rate'] = chunk['int_rate'].str.replace('%', '', regex=False).astype(float)
            
            # 2. Data Quality (Reject Link)
            is_bad = chunk['annual_inc'].isna() | (chunk['annual_inc'] <= 0)
            bad_df = chunk[is_bad].copy()
            good_df = chunk[~is_bad].copy()

            # 3. Load Rejects
            if not bad_df.empty:
                bad_df['error_reason'] = "Invalid Income"
                bad_df.columns = [c.lower() for c in bad_df.columns]
                # We add 'error_reason' to the type dict for this load
                err_types = ORACLE_TYPES.copy()
                err_types['error_reason'] = oracle.VARCHAR2(255)
                bad_df.to_sql('err_loan_data', engine, if_exists='append', index=False, dtype=err_types)
                total_bad += len(bad_df)

            # 4. Load Good Data
            if not good_df.empty:
                good_df.columns = [c.lower() for c in good_df.columns]
                good_df.to_sql('stg_loan_data', engine, if_exists='append', index=False, dtype=ORACLE_TYPES)
                total_good += len(good_df)

            print(f"Batch Processed: Good={len(good_df)}, Bad={len(bad_df)}")
            if (total_good + total_bad) >= 250000: break

        # B. FINAL AUDIT LOG
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text("""
                UPDATE "ETL_AUDIT_LOG" SET "END_TIME" = :ts, "STATUS" = 'SUCCESS', 
                "RECORDS_INSERTED" = :g, "RECORDS_REJECTED" = :b 
                WHERE "JOB_NAME" = 'FIN_LOAN_ETL' AND "STATUS" = 'RUNNING'"""),
                {"ts": datetime.now(), "g": total_good, "b": total_bad}
            )
            conn.commit()
        print("--- Pipeline Finished Successfully ---")

    except Exception as e:
        print(f"FATAL ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    process_etl()