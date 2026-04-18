#!/bin/bash

# --- 1. Configuration and Environment Setup ---
export SOURCE="snies_portal.html"
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd) # Get the directory where the script is located
export DEST_FOLDER="$SCRIPT_DIR/../raw_snies_files" # Set the destination folder to the parent directory of the script

echo "$DEST_FOLDER"
