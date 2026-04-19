# Implement Docker Secrets Task Tracking

## Tasks
- `[x]` **Docker Configuration**
  - `[x]` Define secrets and mount them in `docker-compose.yaml`.
  - `[x]` Update `postgres`, `loader`, and `orchestrator` services.
- `[x]` **Directory Setup & Security**
  - `[x]` Create `secrets/` directory.
  - `[x]` Create placeholder secret files (`db_user`, `db_password`).
  - `[x]` Update `.gitignore` to track secrets directory securely.
- `[x]` **Entrypoint setup**
  - `[x]` Create `entrypoint.sh`.
  - `[x]` Update orchestration Dockerfile or mount for `entrypoint.sh` usage.
- `[x]` **Python integration**
  - `[x]` Create `secrets_manager.py`.
  - `[x]` Refactor `ingest_data.py`.
- `[x]` **DBT Integration**
  - `[x]` Refactor `profiles.yml` to use `env_var`.
- `[x]` **Verification**
  - `[x]` Test python secrets import.
  - `[x]` Explain benefits to the user.
