# Implementation Plan - Restrict File Processing in SNIES ETL

This plan outlines the changes required to implement file filtering in the `file_audit` task of the SNIES ETL pipeline. We will use a configuration file to define allowed prefixes and years.

## User Review Required

> [!IMPORTANT]
> The filtering logic will skip any files in `RAW_FILES_DIR` that do not match both an allowed prefix AND an allowed year. This means previously processed files that don't match the new criteria will no longer be tracked in the returned `new_files` list, even if they change.

## Proposed Changes

### Configuration

#### [NEW] [ingestion_config.json](file:///home/diego/repos/snies-challenge/orchestration/flows/ingestion_config.json)
Create a JSON configuration file to store the allowed prefixes and years.
```json
{
  "allowed_prefixes": ["estudiantes_inscritos", "docentes"],
  "allowed_years": [2022, 2023, 2024]
}
```

---

### Orchestration

#### [MODIFY] [etl_flow.py](file:///home/diego/repos/snies-challenge/orchestration/flows/etl_flow.py)
- Import `json`.
- Add a helper function `load_config()` to read the JSON file.
- Update `file_audit` to:
    1. Load the configuration.
    2. Iterate through files in `RAW_FILES_DIR`.
    3. Check if the filename starts with any of the `allowed_prefixes`.
    4. Check if the filename contains any of the `allowed_years`.
    5. Only proceed with hash calculation and comparison if both conditions are met.

---

## Open Questions

- Should the year check be strict (e.g., must be a 4-digit number surrounded by delimiters) or a simple "contains" check? I will implement a "contains" check for simplicity, assuming the filenames follow the standard SNIES format (e.g., `prefix_year.xlsx`).

## Verification Plan

### Automated Tests
- Run `python orchestration/flows/tests/test_audit.py` to verify that only files matching the new criteria are flagged.
- I will need to update `test_audit.py` if I want to see the "skipped" logs in the test output.

### Manual Verification
- Check the console output of the `orchestrator` container during a flow run to ensure "Skipping file X (not in allowed prefixes/years)" messages appear for non-matching files.
