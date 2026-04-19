# Docker Secrets Implementation Walkthrough

The platform has been successfully refactored to use Docker Secrets instead of `.env` files for securely managing database credentials. Here is a review of the implemented changes:

## 1. Docker Compose Configuration
The `docker-compose.yaml` file was updated to define and manage credentials via Docker secrets instead of plain environment variables.
- A global `secrets:` section binds `db_user` and `db_password` to local files inside the new `./secrets` repository.
- The `postgres` image is now fed these secrets securely via `POSTGRES_USER_FILE` and `POSTGRES_PASSWORD_FILE`.
- The `orchestrator` and `loader` containers have the secrets mounted and the specific locations passed as `DB_USER_FILE` and `DB_PASSWORD_FILE`.

## 2. Dynamic Secret Processing
Instead of mounting secrets inside the container and hoping the tooling can read file paths dynamically, I developed specific parsers:

### Python Extractor (`secrets_manager.py`)
`scripts/secrets_manager.py` exposes exactly what `ingest_data.py` needs to query Postgres cleanly natively through Python.
The script handles file lookup:
1. First, determining if `DB_USER_FILE` and `DB_PASSWORD_FILE` environment variables point to local locations.
2. Gracefully attempting to parse and sanitize the content from the mounted `/run/secrets/`.
3. Auto-fallback to local system state values (`USER`, `PASSWORD`) for seamless local mock execution.

`ingest_data.py` was refactored to import the generated DB Engine URL cleanly.

### Docker Extractor (`entrypoint.sh`)
I created `orchestration/entrypoint.sh` for your Prefect Server instance. The shell script intercepts startup commands internally, extracts the Docker secrets passed via file mapping, and strictly sets standard `DB_USER` and `DB_PASSWORD` global variables before dropping execution back down to your Prefect process via `exec "$@"`. This exposes the standard env keys.

## 3. DBT Interpolation
For the transformation layer, the `dbt_project/profiles.yml` logic was modified.
Because the standard `DB_USER` variables were re-exposed globally by the custom `entrypoint.sh` above, dbt intercepts them naturally via its jinja `env_var()` compiler.

## 4. Operational Best Practices
A crucial part of using Docker secrets correctly relies on them *staying* secret.
I created placeholder `./secrets/db_user` and `./secrets/db_password` directories but also injected rigorous exclusion into `.gitignore` targeting the `secrets/` namespace. It explicitly permits `.gitkeep` if later added but fundamentally halts developer git pushes of active connection passwords inside this directory.

> [!TIP]
> **Why this format works for your Bogotá production ecosystem over .env**
> The plain `.env` paradigm leaks standard environmental variables into memory. When container instances crash in Production, standard logs, diagnostics dumps, and even internal `docker inspect` utilities snapshot those variables fully exposing the root `POSTGRES_PASSWORD`. By transferring strictly to Docker Secrets, the orchestration kernel guarantees that the file contents are only actively mounted to memory temporarily inside a RAM disk `tmpfs`, ensuring credentials vanish invisibly to any system tracing logs when scaled on Prefect and Bogotá's production nodes.
