#!/bin/sh

# 1. Load secrets
export DB_USER=$(cat /run/secrets/db_user)
export DB_PASSWORD=$(cat /run/secrets/db_password)

(
    # Wait for the API to respond
    until curl -s http://localhost:3000/api/health > /dev/null; do
        sleep 2
    done

    echo "Grafana detected. Retrieving internal ID for the fix..."

    # Query the datasource to get current metadata
    DS_JSON=$(curl -s http://localhost:3000/api/datasources/uid/sue_gold_datasource_v3)
    
    # Extract ID using sed (BusyBox compatible)
    DS_ID=$(echo "$DS_JSON" | sed -n 's/.*"id":\([0-9]*\),.*/\1/p')

    if [ -z "$DS_ID" ]; then
        echo "Error: Could not parse ID. Response: $DS_JSON"
    else
        echo "Activating Datasource with UID: sue_gold_datasource_v3 (Internal ID: $DS_ID)..."
        
        # Perform the PUT request to trigger the datasource update/activation
        curl -s -X PUT 'http://localhost:3000/api/datasources/uid/sue_gold_datasource_v3' \
            -H 'Accept: application/json, text/plain, */*' \
            -H 'Content-Type: application/json' \
            -H 'x-grafana-org-id: 1' \
            --data-raw "{
                \"id\": $DS_ID,
                \"uid\": \"sue_gold_datasource_v3\",
                \"orgId\": 1,
                \"name\": \"Postgres_SUE_Final\",
                \"type\": \"grafana-postgresql-datasource\",
                \"access\": \"proxy\",
                \"url\": \"postgres:5432\",
                \"user\": \"$DB_USER\",
                \"database\": \"snies\",
                \"isDefault\": true,
                \"jsonData\": {
                    \"connMaxLifetime\": 14400,
                    \"sslmode\": \"disable\",
                    \"database\": \"snies\",
                    \"maxOpenConns\": 100,
                    \"maxIdleConns\": 100,
                    \"maxIdleConnsAuto\": true
                },
                \"secureJsonData\": {
                    \"password\": \"$DB_PASSWORD\"
                },
                \"version\": 10
            }"
            
        echo ""
        echo "Synchronization sent. The dashboard should be active now."
    fi
) &

# Execute the official Grafana entrypoint
exec /run.sh