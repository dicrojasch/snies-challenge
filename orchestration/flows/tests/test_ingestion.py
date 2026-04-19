import sys
import os
from prefect import flow

# Ensure the parent directory is in the path to import etl_flow
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from etl_flow import run_ingestion, RAW_FILES_DIR


@flow
def debug_ingestion_flow():

    # We pass a list of dicts because run_ingestion expects file_info["file_name"], etc.
    test_files = [
        {
            "file_name": "estudiantes_inscritos_2022.xlsx",
            "path": os.path.join(RAW_FILES_DIR, "estudiantes_inscritos_2022.xlsx"),
            "file_hash": "dummy_test_hash_12345",
        },
        {
            "file_name": "docentes_2022.xlsx",
            "path": os.path.join(RAW_FILES_DIR, "docentes_2022.xlsx"),
            "file_hash": "dummy_test_hash_99876",
        },
    ]

    print(f"Executing run_ingestion task with test files: {test_files}...")

    try:
        result = run_ingestion(test_files)
        print(f"Task finished with result: {result}")
    except Exception as e:
        print(
            f"Task failed. Make sure {os.path.join(RAW_FILES_DIR, 'docentes_2022.xlsx')} \
            and {os.path.join(RAW_FILES_DIR, 'estudiantes_inscritos_2022.xlsx')} exists. Error: {e}"
        )


if __name__ == "__main__":
    debug_ingestion_flow()
