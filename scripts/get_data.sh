#!/bin/bash

# --- 1. Configuration and Environment Setup ---
export SOURCE="snies_portal.html"
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd) # Get the directory where the script is located
export DEST_FOLDER="$SCRIPT_DIR/../raw_snies_files" # Set the destination folder to the parent directory of the script
TARGET_URL="https://snies.mineducacion.gov.co/portal/ESTADISTICAS/Bases-consolidadas/"

# Fetching portal source and extracting the Base URL
echo "Fetching portal source ($TARGET_URL) to extract Base URL..."
SOURCE_CONTENT=$(curl -s --compressed "$TARGET_URL" -H "User-Agent: Mozilla/5.0")
echo "$SOURCE_CONTENT" > "$SOURCE"

export BASE_URL=$(echo "$SOURCE_CONTENT" | grep -oP '(?<=<base href=")[^"]*(?=/w3)')

if [ -z "$BASE_URL" ]; then
    echo "ERROR: Could not extract BASE_URL from $TARGET_URL"
    exit 1
fi

# Create the destination directory if it doesn't exist
mkdir -p "$DEST_FOLDER"

echo "Starting file downloads into: $DEST_FOLDER"

# --- 2. Processing Pipeline ---
# This pipeline cleans the HTML, extracts table cells, and parses the filename and link.
tidy -asxhtml -numeric -q --show-warnings no --indent no "$SOURCE" | \
xmllint --html --xpath "//table//td" - 2>/dev/null | \
tr '\n' ' ' | \
sed 's/<\/td>/<\/td>\n/g' | \
awk '{
    # Extract relative URL (looking for articles-XXXX_recurso pattern)
    if (match($0, /href="([^"]*articles-[^"]+)"/, h)) {
        url = h[1]
        # Extract link text (at least 3 chars long to avoid junk)
        if (match($0, />([^<]{3,})<\/a>/, t)) {
            name = tolower(t[1])
            # Standardize filename
            gsub(/[^a-z0-9]+/, "_", name)
            gsub(/^_+|_+$/, "", name)
            if (name != "" && url != "") print url "|" name
        }
    }
}' | sort -u | \
xargs -I {} -P 4 bash -c '
    LINE="{}"
    FILE_URL="${LINE%|*}"
    BASE_FILENAME="${LINE#*|}"
    
    if [ -z "$FILE_URL" ] || [ -z "$BASE_FILENAME" ]; then exit 0; fi

    EXT="${FILE_URL##*.}"
    # Default to xlsx if extension is missing or weird
    if [[ ! "$EXT" =~ ^(xlsx|xlsb|xls|csv)$ ]]; then EXT="xlsx"; fi
    
    FULL_PATH="${DEST_FOLDER}/${BASE_FILENAME}.${EXT}"

    # stat -c%s for file size in bytes
    CURRENT_SIZE=$(stat -c%s "$FULL_PATH" 2>/dev/null || echo 0)
    
    if [ ! -f "$FULL_PATH" ] || [ "$CURRENT_SIZE" -lt 5120 ]; then
        DOWNLOAD_URL="${BASE_URL%/}/${FILE_URL#/}"
        echo "Downloading: ${BASE_FILENAME}.${EXT} from $DOWNLOAD_URL"
        curl -sL "$DOWNLOAD_URL" -H "User-Agent: Mozilla/5.0" -o "$FULL_PATH"
        
        # Verify download size
        NEW_SIZE=$(stat -c%s "$FULL_PATH" 2>/dev/null || echo 0)
        if [ "$NEW_SIZE" -lt 5120 ]; then
             echo "WARNING: Downloaded ${BASE_FILENAME} is still suspiciously small ($NEW_SIZE bytes)."
        fi
    else
        echo "Skipping: ${BASE_FILENAME} (Already exists and appears valid)"
    fi
' _

