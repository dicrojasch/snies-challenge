from prefect import task, flow
import subprocess
import os
import hashlib
from sqlalchemy import create_engine, text
from datetime import datetime

DB_URL = os.getenv("DATABASE_URL", "postgresql://user:password@postgres:5432/snies")
RAW_FILES_DIR = "/raw_snies_files"

def get_engine():
    return create_engine(DB_URL)

@task(name="Fetch External Data", retries=1)
def fetch_external_data():
    """Execute the bash script to download data."""
    script_path = "/scripts/get_data.sh"
    print(f"Executing {script_path}...")
    try:
        result = subprocess.run(["bash", script_path], capture_output=True, text=True, check=True)
        print("Fetch Output:\n", result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Fetch failed: {e.stdout}\n{e.stderr}")
        raise

@task(name="File Audit & Control")
def file_audit():
    """Scan raw_snies_files, compare hashes with DB, identify new files."""
    engine = get_engine()
    
    if not os.path.exists(RAW_FILES_DIR):
        print(f"Directory {RAW_FILES_DIR} not found.")
        return []

    # Get DB hashes
    query = "SELECT file_name, file_hash FROM bronze.ingestion_audit WHERE status = 'SUCCESS'"
    existing_records = {}
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            for row in result:
                existing_records[row[0]] = row[1]
    except Exception as e:
        print(f"Could not query ingestion_audit table (might be empty/missing first run): {e}")

    new_files = []
    
    for filename in os.listdir(RAW_FILES_DIR):
        filepath = os.path.join(RAW_FILES_DIR, filename)
        if os.path.isfile(filepath):
            # Calculate hash
            hasher = hashlib.sha256()
            with open(filepath, 'rb') as f:
                buf = f.read()
                hasher.update(buf)
            file_hash = hasher.hexdigest()
            
            if filename not in existing_records or existing_records[filename] != file_hash:
                new_files.append({"file_name": filename, "file_hash": file_hash, "path": filepath})
    
    print(f"Found {len(new_files)} new/modified files.")
    return new_files

@task(name="Ingest SNIES Data")
def run_ingestion(new_files):
    """Execute Python ingestion for new files and update audit table."""
    engine = get_engine()
    
    successful_files = []
    for file_info in new_files:
        filename = file_info["file_name"]
        filepath = file_info["path"]
        file_hash = file_info["file_hash"]
        
        print(f"Loading {filename}...")
        try:
            result = subprocess.run(["python", "/scripts/ingest_data.py", "--file", filepath], capture_output=True, text=True, check=True)
            print(result.stdout)
            
            # Update audit table
            with engine.begin() as conn:
                audit_query = text("""
                    INSERT INTO bronze.ingestion_audit (file_name, file_hash, processed_at, status) 
                    VALUES (:file_name, :file_hash, :processed_at, 'SUCCESS')
                    ON CONFLICT (file_name) 
                    DO UPDATE SET file_hash = EXCLUDED.file_hash, processed_at = EXCLUDED.processed_at, status = 'SUCCESS'
                """)
                conn.execute(audit_query, {"file_name": filename, "file_hash": file_hash, "processed_at": datetime.now()})
                
            successful_files.append(filename)
            
        except subprocess.CalledProcessError as e:
            print(f"Ingestion failed for {filename}: {e.stdout}\n{e.stderr}")
            with engine.begin() as conn:
                audit_query = text("""
                    INSERT INTO bronze.ingestion_audit (file_name, file_hash, processed_at, status) 
                    VALUES (:file_name, :file_hash, :processed_at, 'FAILED')
                    ON CONFLICT (file_name) 
                    DO UPDATE SET status = 'FAILED'
                """)
                conn.execute(audit_query, {"file_name": filename, "file_hash": file_hash, "processed_at": datetime.now()})
            raise # Fail the task if a file load fails
            
    return successful_files

@task(name="Run dbt Models", retries=1)
def run_dbt():
    """Run dbt build to materialize silver and gold schemas."""
    print("Starting dbt transformations...")
    try:
        result = subprocess.run(
            ["dbt", "build", "--profiles-dir", ".", "--project-dir", "/dbt_project"],
            cwd="/dbt_project",
            capture_output=True,
            text=True,
            check=True
        )
        print("dbt Output:\n", result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"dbt build failed: {e.stdout}\n{e.stderr}")
        raise

@flow(name="SNIES End-to-End ETL Flow")
def snies_etl():
    """Main Orchestration Flow with File Sensing and Audit."""
    # Step 1: Fetch External Data
    fetch_external_data()
    
    # Step 2: File Audit & Control
    new_files = file_audit()
    
    # Step 3: Conditional Flow Control
    if new_files:
        print(f"Detected {len(new_files)} new files. Proceeding with ingestion...")
        run_ingestion(new_files)
        run_dbt()
    else:
        print("No new data found. Skipping ingestion and dbt models.")

if __name__ == "__main__":
    snies_etl()
