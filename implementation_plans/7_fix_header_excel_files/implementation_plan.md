# Implementation Plan - Dynamic Excel Header Detection

The goal is to enhance the `scripts/ingest_data.py` script to handle Excel files where the data table doesn't start at the first row. We will implement a robust mechanism to identify the header row based on known keywords.

## User Review Required

> [!IMPORTANT]
> I will use a list of common SNIES headers (e.g., 'CÓDIGO', 'NOMBRE', 'AÑO', 'MATRÍCULA') as keywords. Please let me know if there are specific keywords you'd like to use instead.

## Proposed Changes

### Ingestion Script

#### [MODIFY] [ingest_data.py](file:///home/diego/repos/snies-challenge/scripts/ingest_data.py)

- Add a new helper function `read_excel_with_dynamic_header(file_path, engine, keywords)` that:
    - Reads the first few rows of the file (e.g., top 20).
    - Searches for the header row by matching any of the provided keywords.
    - Re-reads the file from the identified header row.
- Update `load_file` to use this new function for Excel files.

## Verification Plan

### Automated Tests
- I will create a dummy Excel file with metadata headers to verify the dynamic detection logic.
- Run `python scripts/ingest_data.py --file [test_file]` and check if the data is loaded correctly.

### Manual Verification
- Verify logs to see if the header index is correctly identified.
