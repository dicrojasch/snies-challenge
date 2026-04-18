from prefect import task, flow, get_run_logger
import subprocess
import os
import hashlib
import json
from sqlalchemy import create_engine, text
from datetime import datetime

DB_URL = os.getenv("DATABASE_URL", "postgresql://user:password@postgres:5432/snies")
RAW_FILES_DIR = "/raw_snies_files"

def get_engine():
    return create_engine(DB_URL)

def load_ingestion_config():
    """Load allowed prefixes and years from config file."""
    config_path = os.path.join(os.path.dirname(__file__), "ingestion_config.json")
    if not os.path.exists(config_path):
        # Fallback defaults if config is missing
        return {
            "allowed_prefixes": ["estudiantes_inscritos", "docentes"],
            "allowed_years": [2022, 2023, 2024]
        }
    with open(config_path, "r") as f:
        return json.load(f)

@task(name="Fetch External Data", retries=1)
def fetch_external_data():
    """Execute the bash script to download data."""
    logger = get_run_logger()
    script_path = "/scripts/get_data.sh"
    logger.info(f"Executing {script_path}...")
    try:
        result = subprocess.run(["bash", script_path], capture_output=True, text=True, check=True)
        logger.debug(f"Fetch Output:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Fetch failed: {e.stdout}\n{e.stderr}")
        raise

@task(name="File Audit & Control")
def file_audit():
    """Scan raw_snies_files, compare hashes with DB, identify new files based on criteria."""
    logger = get_run_logger()
    engine = get_engine()
    config = load_ingestion_config()
    prefixes = config.get("allowed_prefixes", [])
    years = [str(y) for y in config.get("allowed_years", [])]
    
    if not os.path.exists(RAW_FILES_DIR):
        logger.warning(f"Directory {RAW_FILES_DIR} not found.")
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
        logger.error(f"Database connection failed or could not query ingestion_audit table: {e}")

    new_files = []
    
    for filename in os.listdir(RAW_FILES_DIR):
        filepath = os.path.join(RAW_FILES_DIR, filename)
        
        # 1. Filter by prefix and year
        matches_prefix = any(filename.lower().startswith(p.lower()) for p in prefixes)
        matches_year = any(y in filename for y in years)
        
        if not (matches_prefix and matches_year):
            logger.debug(f"Skipping {filename} (Criteria no match)")
            continue

        if os.path.isfile(filepath):
            # Calculate hash
            logger.debug(f"Calculating hash for {filename}")
            hasher = hashlib.sha256()
            with open(filepath, 'rb') as f:
                buf = f.read()
                hasher.update(buf)
            file_hash = hasher.hexdigest()
            
            if filename not in existing_records or existing_records[filename] != file_hash:
                new_files.append({"file_name": filename, "file_hash": file_hash, "path": filepath})
            else:
                logger.warning(f"File already processed, skipping: {filename}")
    
    logger.info(f"Audit completed. Found {len(new_files)} new/modified files meeting criteria.")
    return new_files

@task(name="Ingest SNIES Data")
def run_ingestion(new_files):
    """Execute Python ingestion for new files and update audit table."""
    logger = get_run_logger()
    engine = get_engine()
    
    successful_files = []
    for file_info in new_files:
        filename = file_info["file_name"]
        filepath = file_info["path"]
        file_hash = file_info["file_hash"]
        
        logger.info(f"Starting ingestion for {filename}...")
        try:
            result = subprocess.run(["python", "/scripts/ingest_data.py", "--file", filepath], capture_output=True, text=True, check=True)
            logger.debug(result.stdout)
            
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
            logger.error(f"Ingestion failed for {filename}: {e.stdout}\n{e.stderr}")
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
    logger = get_run_logger()
    logger.info("Starting dbt transformations...")
    try:
        result = subprocess.run(
            ["dbt", "build", "--profiles-dir", ".", "--project-dir", "/dbt_project"],
            cwd="/dbt_project",
            capture_output=True,
            text=True,
            check=True
        )
        logger.debug(f"dbt Output:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt build failed: {e.stdout}\n{e.stderr}")
        raise

@flow(name="SNIES End-to-End ETL Flow")
def snies_etl():
    """Main Orchestration Flow with File Sensing and Audit."""
    logger = get_run_logger()
    # Step 1: Fetch External Data
    fetch_external_data()
    
    # Step 2: File Audit & Control
    new_files = file_audit()
    
    # Step 3: Conditional Flow Control
    if new_files:
        logger.info(f"Detected {len(new_files)} new files. Proceeding with ingestion...")
        run_ingestion(new_files)
        run_dbt()
    else:
        logger.info("No new data found. Skipping ingestion and dbt models.")

if __name__ == "__main__":
    snies_etl()
