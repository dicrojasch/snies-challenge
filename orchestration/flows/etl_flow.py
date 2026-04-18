from prefect import task, flow
import subprocess
import os

@task(name="Ingest SNIES Data", retries=2, retry_delay_seconds=60)
def run_ingestion():
    """Execute the Python ingestion script."""
    print("Starting raw data ingestion into Bronze schema...")
    # This calls the ingest_data script inside the orchestrator container or via an API
    # Since we are in the orchestrator container and have the scripts folder mapped via volumes (if needed)
    # Actually, the python script is in /scripts/ingest_data.py
    try:
        # Assuming we can just run it using subprocess if python and required libs are installed
        # In a real microservice arch, we'd trigger the loader service or run a kubernetes job
        result = subprocess.run(["python", "/scripts/ingest_data.py"], capture_output=True, text=True, check=True)
        print("Ingestion Output:\n", result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Ingestion failed: {e.stderr}")
        raise

@task(name="Run dbt Models", retries=1)
def run_dbt():
    """Run dbt build to materialize silver and gold schemas."""
    print("Starting dbt transformations...")
    # dbt needs to be run from inside the dbt_project directory
    try:
        # We specify the profile directory if needed, defaulting to local
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
    """Main Orchestration Flow for SNIES Data Challenge."""
    # Step 1: Ingest Data
    run_ingestion()
    # Step 2: Transform Data
    run_dbt()

if __name__ == "__main__":
    snies_etl()
