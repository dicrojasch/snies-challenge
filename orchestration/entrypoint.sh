#!/bin/bash
set -e

# Load secrets into environment variables if files exist
if [ -f "${DB_USER_FILE}" ]; then
    export DB_USER=$(cat "${DB_USER_FILE}")
fi

if [ -f "${DB_PASSWORD_FILE}" ]; then
    export DB_PASSWORD=$(cat "${DB_PASSWORD_FILE}")
fi

# Fallback values for local dev (if not overridden by secrets)
export DB_USER=${DB_USER:-user}
export DB_PASSWORD=${DB_PASSWORD:-password}
export DB_HOST=${DB_HOST:-postgres}
export DB_PORT=${DB_PORT:-5432}
export DB_NAME=${DB_NAME:-snies}

# Construct the SQLAlchemy database URL
export DATABASE_URL="postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"

# Execute the command passed to the container
exec "$@"
