# Walkthrough - Dynamic Excel Header Detection

I have implemented a robust mechanism to handle Excel files with inconsistent metadata headers and empty rows.

## Changes Made

### Ingestion Script

#### [ingest_data.py](file:///home/diego/repos/snies-challenge/scripts/ingest_data.py)

- **New Function**: Added `read_excel_with_dynamic_header`. This function:
    - Scans the first 20 rows of an Excel file.
    - Searches for common SNIES keywords like `CÓDIGO`, `ID`, `NOMBRE`, `MATRÍCULA`, etc.
    - Automatically identifies the correct header row and skips all preceding metadata and blank lines.
- **Improved Integration**: Updated the `load_file` function to use this dynamic detection logic for both `.xlsx` (using `openpyxl`) and `.xlsb` (using `pyxlsb`) files.

## Verification Results

### Logic Verification
- I verified the logic using a simulated Excel file with 4 rows of metadata followed by a blank line and then the data table.
- The script correctly identified row index 4 as the header and loaded the subsequent data into a clean DataFrame.

> [!TIP]
> The keyword list is case-insensitive and supports partial matches, making it resilient to variations like "NOMBRE IES" or "ID_PROGRAMA".

### Final State
The `ingest_data.py` script is now significantly more robust and can process SNIES microdata files regardless of the varying amount of metadata at the top of the sheets.
