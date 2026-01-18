#!/bin/bash

echo "--- [$(date)] Starting Financial ETL Pipeline ---"

# Step 1: Run the Python ETL
python loan_etl.py

# Step 2: Check if successful
if [ $? -eq 0 ]; then
    echo "SUCCESS: Data loaded to Staging."
else
    echo "FAILURE: ETL script crashed."
    exit 1
fi

echo "--- [$(date)] Pipeline Finished ---"