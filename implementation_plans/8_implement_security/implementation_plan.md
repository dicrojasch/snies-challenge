# Implement Docker Secrets for the SNIES Data Platform

This plan addresses the user's request to refactor the data platform to use Docker Secrets, improving security over the plain `.env` approach.

## Summary of Changes
- Define and configure Docker Secrets in `docker-compose.yaml`.
- Create an `entrypoint.sh` script to parse secrets for the Prefect/dbt orchestrator container.
- Create a Python utility module `secrets_manager.py` to safely extract secrets from the file system.
- Implement a `.gitignore` update for the `./secrets/` directory to prevent accidental commits.
- Modify `dbt_project/profiles.yml` to use `env_var`.

## Proposed Changes

### Docker Configuration

#### [MODIFY] [docker-compose.yaml](file:///home/diego/repos/snies-challenge/docker-compose.yaml)
- Add top-level `secrets` configuration.
- Update `postgres` service to use `POSTGRES_USER_FILE` and `POSTGRES_PASSWORD_FILE`.
- Add `secrets` to `loader` and `orchestrator` services.
- Define `DB_USER_FILE` and `DB_PASSWORD_FILE` in the environment of `loader` and `orchestrator`.
- Add an `entrypoint` to `orchestrator` pointing to `/orchestration/entrypoint.sh`.

#### [NEW] [entrypoint.sh](file:///home/diego/repos/snies-challenge/orchestration/entrypoint.sh)
- Extract credentials from the files mounted at `/run/secrets/` or defined by `DB_USER_FILE`, and `DB_PASSWORD_FILE`, and export them as standard environment variables (`DB_USER`, `DB_PASSWORD`, `DATABASE_URL`).
- Execute `exec "$@"` to pass control back to the original docker command.

### Python Code

#### [NEW] [secrets_manager.py](file:///home/diego/repos/snies-challenge/scripts/secrets_manager.py)
- Create `get_secret()` to try reading from `.txt` secret files in `/run/secrets/` first, before falling back to local host environment variables.
- Create `get_database_url()` to dynamically build the `postgresql://` string.

#### [MODIFY] [ingest_data.py](file:///home/diego/repos/snies-challenge/scripts/ingest_data.py)
- Replace static `DB_URL = os.getenv("DATABASE_URL")` with parsing logic from `secrets_manager.py`.

### dbt Integration

#### [MODIFY] [profiles.yml](file:///home/diego/repos/snies-challenge/dbt_project/profiles.yml)
- Adjust the DB configuration to load from dynamic environment variables injected by `entrypoint.sh`:
  - `user: "{{ env_var('DB_USER', 'user') }}"`
  - `password: "{{ env_var('DB_PASSWORD', 'password') }}"`

### Git Configuration

#### [MODIFY] [.gitignore](file:///home/diego/repos/snies-challenge/.gitignore)
- Add a protective rule `secrets/` to ensure secrets are never stored in the repository.

## Open Questions
- Should the default secret files directory `./secrets/` exist prior to running docker-compose, or would you like me to create the empty directory structure for these files now?

## Verification Plan
1. Check if `docker-compose.yaml` validates against the Docker Compose specification.
2. Confirm the `secrets/` folder is ignored by git.
3. Test that Python fallback reading triggers successfully, defaulting safely if no secrets are mounted locally.
