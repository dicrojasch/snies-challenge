import pandas as pd
from sqlalchemy import create_engine
import os
import requests

# Constants and Configuration
DB_URL = os.getenv("DATABASE_URL", "postgresql://user:password@postgres:5432/snies")
FILES_DIR = "/files"

def get_engine():
    """Create database engine."""
    return create_engine(DB_URL)

def load_2022_data():
    """
    Load existing 2022 dataset from local storage to Bronze schema.
    Reads Excel files from /files directory.
    """
    engine = get_engine()
    
    if not os.path.exists(FILES_DIR):
        print(f"Error: Directory {FILES_DIR} not found.")
        return

    print("Checking for local files...")
    files = [f for f in os.listdir(FILES_DIR) if f.endswith('.xlsx') or f.endswith('.csv')]
    
    if not files:
        print(f"No valid data files found in {FILES_DIR}.")
        return

    for file_name in files:
        file_path = os.path.join(FILES_DIR, file_name)
        print(f"Loading {file_name} into Bronze schema...")
        
        # Determine table name based on file name without extension
        table_name = os.path.splitext(file_name)[0].lower()
        
        try:
            if file_name.endswith('.xlsx'):
                df = pd.read_excel(file_path)
            else:
                df = pd.read_csv(file_path)
            
            # Write data to the 'bronze' schema
            df.to_sql(name=table_name, con=engine, schema='bronze', if_exists='replace', index=False)
            print(f"Success: Loaded {len(df)} rows into bronze.{table_name}")
            
        except Exception as e:
            print(f"Error loading {file_name}: {e}")

def download_new_data():
    """
    Placeholder function to download SNIES data for 2023 - 2024.
    Once URLs are identified, they will be scraped or downloaded here directly.
    """
    print("Initiating download for 2023-2024 datasets...")
    # TODO: Implement dataset scraping/downloading mechanism
    print("Download placeholder completed.")

def main():
    print("Starting Data Ingestion Pipeline...")
    load_2022_data()
    download_new_data()
    print("Pipeline execution finished.")

if __name__ == '__main__':
    main()
