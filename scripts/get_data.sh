#!/bin/bash

# --- 1. Configuration and Environment Setup ---
export SOURCE="snies_portal.html"
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd) # Get the directory where the script is located
export DEST_FOLDER="$SCRIPT_DIR/../raw_snies_files" # Set the destination folder to the parent directory of the script
TARGET_URL="https://snies.mineducacion.gov.co/portal/ESTADISTICAS/Bases-consolidadas/"

# Fetching portal source and extracting the Base URL
echo "Fetching portal source ($TARGET_URL) to extract Base URL..."
export BASE_URL=$(curl -s --compressed "$TARGET_URL" \
    -H "User-Agent: Mozilla/5.0" | tee "$SOURCE" | grep -oP '(?<=<base href=")[^"]*(?=/w3)')

# Create the destination directory if it doesn't exist
mkdir -p "$DEST_FOLDER"

echo "Starting file downloads into: $DEST_FOLDER"

# --- 2. Processing Pipeline ---
# This pipeline cleans the HTML, extracts table cells, flattens them into single lines,
# and parses the filename and link text.
tidy -asxhtml -numeric -q --show-warnings no --indent no "$SOURCE" | \
xmllint --html --xpath "//table//td" - 2>/dev/null | \
tr '\n' ' ' | \
sed 's/<\/td>/<\/td>\n/g' | \
awk '{
    match($0, /href="([^"]+)"/, h); 
    match($0, />([^<]+)<\/a>/, t); 
    n=tolower(t[1]); 
    gsub(/[[:space:]]+/, "_", n); 
    if(h[1]) print h[1]"\t"n
}' | \
xargs -n 2 -P 4 bash -c '
    # Assigning arguments to descriptive variables
    FILE_URL="$1"
    BASE_FILENAME="$2"
    
    # Extract extension from URL (e.g., xlsx, xlsb)
    EXT="${FILE_URL##*.}"
    FULL_PATH="${DEST_FOLDER}/${BASE_FILENAME}.${EXT}"

    # Only download if the file does not already exist locally
    if [ ! -f "$FULL_PATH" ]; then
        echo "Downloading: ${BASE_FILENAME}.${EXT}..."
        curl -sL "${BASE_URL}${FILE_URL}" -o "$FULL_PATH"
    else
        echo "Skipping: ${BASE_FILENAME} (Already exists)"
    fi
' _

