"""
Utility module for managing configuration and secrets for the SNIES platform.
It transparently handles reading from Docker Secrets files with a fallback to environment variables.
"""
import os
import logging

logger = logging.getLogger(__name__)

def get_secret(secret_name: str, default: str = None) -> str:
    """
    Reads a secret value from a file (useful for Docker secrets) or
    an environment variable.
    
    Expected environment setup:
    If secret_name is 'db_user', it checks DB_USER_FILE.
    If DB_USER_FILE is not set, it defaults to /run/secrets/db_user.
    If the file exists, its content is returned.
    Otherwise, it checks the environment variable DB_USER.
    Finally, it falls back to the default value provided.
    """
    file_env_var = f"{secret_name.upper()}_FILE"
    secret_path = os.getenv(file_env_var, f"/run/secrets/{secret_name.lower()}")
    
    if os.path.exists(secret_path):
        try:
            with open(secret_path, 'r') as f:
                return f.read().strip()
        except IOError as e:
            logger.warning(f"Error reading secret file {secret_path}: {e}")
            
    # Fallback to direct environment variable
    return os.getenv(secret_name.upper(), default)


def get_database_url() -> str:
    """
    Constructs the SQLAlchemy database URL from secrets and environment variables.
    """
    db_user = get_secret('db_user', 'user')
    db_password = get_secret('db_password', 'password')
    db_host = os.getenv('DB_HOST', 'postgres')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'snies')
    
    return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
