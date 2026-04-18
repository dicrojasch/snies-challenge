import pandas as pd
from sqlalchemy import create_engine
import os
import requests
import tempfile
import re

# Constants and Configuration
DB_URL = os.getenv("DATABASE_URL", "postgresql://user:password@postgres:5432/snies")
FILES_DIR = "/files"

# Sample Placeholder URLs for 2023 and 2024 (To be replaced with actual SNIES dataset endpoints later)
SNIES_DATA_URLS = {
    "estudiantes_inscritos_2023": "https://example.com/estudiantes_inscritos_2023.csv",
    "docentes_2023": "https://example.com/docentes_2023.csv",
    "estudiantes_inscritos_2024": "https://example.com/estudiantes_inscritos_2024.csv",
    "docentes_2024": "https://example.com/docentes_2024.csv"
}

def get_engine():
    """Create database engine."""
    return create_engine(DB_URL)

def format_columns(df):
    """Normalize dataframe columns to standard snake_case."""
    df.columns = [
        re.sub(r'\W+', '_', str(col).strip().lower()).strip('_')
        for col in df.columns
    ]
    return df

def load_local_data():
    """
    Load existing 2022 dataset from local storage to Bronze schema.
    Reads Excel files from /files directory.
    """
    engine = get_engine()
    
    if not os.path.exists(FILES_DIR):
        print(f"Error: Directory {FILES_DIR} not found.")
        return

    print("Checking for local 2022 files...")
    files = [f for f in os.listdir(FILES_DIR) if f.endswith('.xlsx') or f.endswith('.csv')]
    
    if not files:
        print(f"No valid data files found in {FILES_DIR}.")
        return

    for file_name in files:
        file_path = os.path.join(FILES_DIR, file_name)
        print(f"Loading {file_name} into Bronze schema...")
        
        table_name = os.path.splitext(file_name)[0].lower()
        
        try:
            if file_name.endswith('.xlsx'):
                df = pd.read_excel(file_path)
            else:
                df = pd.read_csv(file_path)
            
            df = format_columns(df)
            df.to_sql(name=table_name, con=engine, schema='bronze', if_exists='replace', index=False)
            print(f"Success: Loaded {len(df)} rows into bronze.{table_name}")
            
        except Exception as e:
            print(f"Error loading {file_name}: {e}")

def download_and_load_new_data():
    """
    Downloads SNIES data for 2023 - 2024 and loads it into the Database.
    """
    engine = get_engine()
    print("Initiating download for 2023-2024 datasets...")

    for table_name, url in SNIES_DATA_URLS.items():
        print(f"Downloading {table_name} from {url}...")
        try:
            # We mock the download logic. If this was a real endpoint:
            # response = requests.get(url)
            # response.raise_for_status()
            
            print(f"[MOCK] Downloaded {table_name}. (Skipping real download to avoid 404 on dummy URL)")
            # Simulated dataframe insertion behavior:
            # df = pd.read_csv(io.StringIO(response.text))
            # df = format_columns(df)
            # df.to_sql(name=table_name, con=engine, schema='bronze', if_exists='replace', index=False)
            # print(f"Success: Loaded into bronze.{table_name}")

        except Exception as e:
            print(f"Error downloading {table_name}: {e}")

def main():
    print("Starting Data Ingestion Pipeline...")
    load_local_data()
    download_and_load_new_data()
    print("Pipeline execution finished.")

if __name__ == '__main__':
    main()
