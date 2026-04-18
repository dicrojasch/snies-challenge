import argparse
import pandas as pd
from sqlalchemy import create_engine
import os
import re
import logging
import sys

# Configure standard logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

DB_URL = os.getenv("DATABASE_URL", "postgresql://user:password@postgres:5432/snies")

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

def load_file(file_path):
    """Loads a single file into the bronze schema."""
    engine = get_engine()
    file_name = os.path.basename(file_path)
    
    logger.info(f"Opening file and loading {file_name} into Bronze schema...")
    
    # Determine table name based on file name without extension
    table_name, ext = os.path.splitext(file_name)
    table_name = table_name.lower()
    # Ensure standard table name conventions
    table_name = re.sub(r'\W+', '_', table_name).strip('_')
    
    try:
        if ext.lower() in ['.xlsx', '.xlsb']:
            # Guard: check for HTML content (common in 404/redirects mislabeled as Excel)
            with open(file_path, 'rb') as f:
                header = f.read(500)
                if b"<!DOCTYPE" in header.upper() or b"<HTML" in header.upper():
                    raise ValueError(f"File {file_name} appears to be an HTML error page, not a valid Excel file. Please check if the source URL is correct.")
            
            # Explicitly specify engine as requested by Pandas error message
            engine_name = 'openpyxl' if ext.lower() == '.xlsx' else 'pyxlsb'
            df = pd.read_excel(file_path, engine=engine_name)
        else:
            df = pd.read_csv(file_path)
        
        df = format_columns(df)
        
        # Add loaded_at timestamp for traceability
        df['loaded_at'] = pd.Timestamp.now()
        
        # Log DataFrame info before committing to Postgres
        logger.debug(f"Data Profile for {table_name}:\nColumns and Data Types:\n{df.dtypes}\n\nFirst 5 rows:\n{df.head(5)}\n")
        
        # Write data to the 'bronze' schema
        df.to_sql(name=table_name, con=engine, schema='bronze', if_exists='replace', index=False)
        logger.info(f"Task successfully completed: Loaded {len(df)} rows into bronze.{table_name}")
        
    except Exception as e:
        logger.error(f"Error loading {file_name}: {e}")
        raise

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest SNIES file into PostgreSQL.")
    parser.add_argument('--file', type=str, help='Absolute path to the file to process.', required=True)
    args = parser.parse_args()
    
    load_file(args.file)
