import sys
import os
from prefect import flow

# Ensure the parent directory is in the path to import etl_flow
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from etl_flow import fetch_external_data, RAW_FILES_DIR

@flow
def debug_fetch_flow():
    print("Executing fetch_external_data task...")
    result = fetch_external_data()
    print(f"Task finished.")
    
    # Verify if files appear in the destination folder
    if os.path.exists(RAW_FILES_DIR):
        files = os.listdir(RAW_FILES_DIR)
        print(f"Verification: Found {len(files)} files in {RAW_FILES_DIR}.")
        if files:
            print("First 5 files:", files[:5])
    else:
        print(f"Directory {RAW_FILES_DIR} not found.")

if __name__ == "__main__":
    debug_fetch_flow()
