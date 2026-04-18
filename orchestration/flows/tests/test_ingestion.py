import sys
import os
from prefect import flow

# Ensure the parent directory is in the path to import etl_flow
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from etl_flow import run_ingestion, RAW_FILES_DIR

@flow
def debug_ingestion_flow():
    # Only testing with a mock representation of the 2022 dataset based on etl_flow logic
    target_filename = "2022_MATRICULADOS.xlsx" # Substitute with actual filename based on raw_snies_files if needed
    target_filepath = os.path.join(RAW_FILES_DIR, target_filename)
    
    # We pass a list of dicts because run_ingestion expects file_info["file_name"], etc.
    test_files = [
        {
            "file_name": target_filename,
            "path": target_filepath,
            "file_hash": "dummy_test_hash_123"
        }
    ]
    
    print(f"Executing run_ingestion task with test file: {target_filename}...")
    
    try:
        result = run_ingestion(test_files)
        print(f"Task finished with result: {result}")
    except Exception as e:
         print(f"Task failed. Make sure {target_filepath} exists. Error: {e}")

if __name__ == "__main__":
    debug_ingestion_flow()
