# /orchestration/test_connection.py
from prefect import flow, get_run_logger

@flow(name="Connectivity Check")
def check_ui():
    logger = get_run_logger()
    logger.info("If you see this, the UI connection is WORKING! 🚀")

if __name__ == "__main__":
    check_ui()
