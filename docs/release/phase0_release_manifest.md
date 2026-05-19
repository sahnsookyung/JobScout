# Phase 0 Release Manifest

Last updated: 2026-05-17

This manifest tracks the stabilization branch for the current broad JobScout worktree. It exists so CI, SonarQube, security scans, rollback, and the later `jobscout-cloud` submodule bump can all be tied to exact commits instead of prior green runs.

## Baseline

- Baseline branch: `main`
- Baseline upstream: `origin/main`
- Baseline SHA: `247c2b76`
- Working branch: `codex/implement-release-plan`
- Release status: local stabilization in progress

## Dirty Worktree Inventory

Current changes span these areas and should be split into reviewable commits/PRs before release:

1. CI and container hygiene
   - `.github/workflows/ci.yml`
   - `.github/workflows/sonarqube.yml`
   - `docker-compose*.yml`
   - `scripts/run_tests.py`

2. Fetch source API and JobSpy health
   - `config.yaml`
   - `core/config_loader.py`
   - `core/scraper/jobspy_client.py`
   - `web/backend/models/responses.py`
   - `web/backend/routers/pipeline.py`
   - matching backend/frontend tests

3. Notification reliability and settings
   - `notification/channels.py`
   - `notification/service.py`
   - `notification/user_settings.py`
   - notification backend/frontend tests

4. Web backend and dashboard serving
   - `web/backend/app.py`
   - `web/backend/config.py`
   - `web/backend/dependencies.py`
   - web tests

5. Frontend source UI and production API config
   - `web/frontend/.env.production`
   - `web/frontend/src/features/dashboard/components/DashboardControls.tsx`
   - `web/frontend/src/features/dashboard/components/FetchSourcesPanel.tsx`
   - `web/frontend/src/services/pipelineApi.ts`
   - `web/frontend/src/types/api.ts`
   - frontend tests and CSS

6. Performance and architecture documentation
   - `services/orchestrator/main.py`
   - `docs/current_architecture_spec.md`
   - `docs/release/phase0_release_manifest.md`

## Required Local Gates

Run the relevant subset for each PR and the full set before release:

```bash
uv run python -m pytest tests/unit -q
uv run python -m pytest tests/integration/database/test_orm_schema_snapshot.py -q
npm --prefix web/frontend run type-check
npm --prefix web/frontend run lint
npm --prefix web/frontend run test -- --run
npm --prefix web/frontend run build
docker compose -f docker-compose.test.yml config -q
docker compose -f docker-compose.yml -f docker-compose.microservices.yml -f docker-compose.web.yml --profile web config -q
docker compose -f docker-compose.yml -f docker-compose.microservices.yml -f docker-compose.web.yml -f docker-compose.e2e.yml --profile web config -q
```

Browser smoke is required when frontend or FastAPI serving changes:

1. Serve the built frontend through `web.backend.app:create_app`.
2. Open `/dashboard`.
3. Verify source cards load from same-origin `/api`.
4. Search a source such as `linkedin`.
5. Verify the list filters locally and no console errors appear.

## Required Remote Evidence

Each release PR must record:

- OSS commit SHA
- GitHub Actions run URL for `CI Gate`
- GitHub Actions run URL for `Security Gate`
- SonarQube analysis URL/result for the exact SHA
- Any skipped job and why it is an allowed skip
- Browser smoke evidence when applicable

Before updating `jobscout-cloud`, record:

- OSS SHA being consumed
- `jobscout-cloud` SHA
- submodule pointer SHA
- cloud CI run URL
- migration version/checksum state

## Rollback Checklist

Application rollback:

1. Record last known-good OSS SHA before deploy.
2. Record last known-good `jobscout-cloud` SHA and submodule SHA.
3. Revert the cloud submodule pointer if the hosted wrapper consumed the new OSS SHA.
4. Redeploy the previous image digest or previous source SHA.
5. Verify `/health`, `/api/cloud/health`, login, dashboard load, and task status reads.

Database rollback:

1. If schema changed, take a backup before deploy.
2. Prefer restore/recreate/import over unsupported downgrade assumptions.
3. Verify schema snapshot/checksum after restore.

Configuration rollback:

1. Revert frontend API base URL or runtime config changes.
2. Revert Docker env/container naming only if the rollback target expects old names.
3. Revert notification channel config and secrets through the configured secret path, not source control.

Rollback trigger examples:

- CI/Sonar fails on the release SHA.
- Built dashboard cannot reach same-origin API.
- Task state cannot be read after orchestrator restart.
- Tenant/auth isolation tests fail in `jobscout-cloud`.
- Notification delivery produces duplicate or untracked external sends.

