import sys
import os
from prefect import flow

# Ensure the parent directory is in the path to import etl_flow
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from etl_flow import file_audit


@flow
def debug_audit_flow():
    print("Executing file_audit task...")
    new_files = file_audit()
    print(f"Task finished. Found {len(new_files)} new/modified files.")

    if new_files:
        print(f"\nfound {len(new_files)} new files to ingest")
        print("\nDetails of the first 6 flagged files:")
        for file_info in new_files[:6]:
            print(f"- {file_info['file_name']} (Hash: {file_info['file_hash']})")
        print(
            "\nThese were flagged because their hash doesn't exist in 'bronze.ingestion_audit' or the hash changed."
        )


if __name__ == "__main__":
    debug_audit_flow()
