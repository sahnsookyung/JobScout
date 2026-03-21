# JobScout Architecture

> Generated from the GitNexus knowledge graph (4,049 symbols · 10,597 edges · 191 execution flows · 281 files)

---

## Overview

JobScout is an AI-powered job matching platform that scrapes job postings, extracts structured requirements via LLM, embeds them into vector space, and scores them against a candidate's resume. It ships in two runtime modes:

| Mode | Entry point | When to use |
|------|------------|-------------|
| **Monolith** | `main.py` | Local development, single-process sequential run |
| **Microservices** | `docker-compose.microservices.yml` | Production — each stage runs as an independent FastAPI service |

Both modes share the same domain logic, repositories, and database schema. The microservices mode adds Redis Streams for async inter-service messaging.

---

## Functional Areas

Ranked by symbol count from the knowledge graph:

| Cluster | Symbols | Cohesion | Description |
|---------|---------|----------|-------------|
| **Services** | 336 | 81% | Four FastAPI microservice entrypoints (orchestrator, extraction, embeddings, scorer-matcher) and their shared base logic |
| **Notification** | 235 | 87% | Notification pipeline: deduplication, channel dispatch (Discord, Email, in-app), message building, RQ worker |
| **Pipeline** | 135 | 74% | End-to-end matching runner: loads user wants, runs vector search, scores, saves results |
| **Scorer** | 88 | 90% | Fit scoring, penalty calculation, want-score weighting, explainability |
| **Etl** | 80 | 83% | Job ETL service: upsert, fingerprinting, content hashing, facet extraction |
| **Routers** | 61 | 91% | FastAPI route handlers for pipeline control, matches, notifications, policy |
| **Repositories** | 59 | 91% | SQLAlchemy repository layer for all DB models |
| **Resume** | 48 | 88% | Resume parsing, evidence extraction, embedding store |
| **Orchestrator** | 47 | 79% | ETL orchestrator: scheduled scraping, retry logic, stage coordination |
| **Llm** | 46 | 94% | LLM provider abstractions (OpenAI), schema models, structured extraction |
| **Matcher** | 37 | 90% | Two-stage vector matching: coarse summary-embedding ANN → requirement-level reranking |
| **Config\_loader** | 18 | 100% | YAML-driven config: matcher, scorer, facet weights, result policy |
| **Backend** | 16 | 100% | Web backend app setup, middleware, lifespan |

---

## System Diagram

```mermaid
graph TB
    subgraph Ingestion
        SCRAPER[Job Scraper<br/>jobspy / custom sites]
    end

    subgraph "Microservices (Redis Streams)"]
        ORCH[Orchestrator Service<br/>:8080<br/>schedules scraping &amp; stages]
        EXTR[Extraction Service<br/>:8081<br/>LLM structured extraction]
        EMBD[Embeddings Service<br/>:8082<br/>vector embedding]
        SCMATCH[Scorer-Matcher Service<br/>:8083<br/>ANN search + scoring]
    end

    subgraph "Core Domain"
        ETL[ETL / Orchestrator<br/>etl/orchestrator.py]
        RESUME[Resume Profiler<br/>etl/resume/]
        MATCHER[Matcher Service<br/>core/matcher/]
        SCORER[Scoring Service<br/>core/scorer/]
        LLM[LLM Provider<br/>core/llm/]
    end

    subgraph "Data Layer"
        PG[(PostgreSQL<br/>+ pgvector)]
        REDIS[(Redis<br/>Streams + Pub/Sub)]
        REPOS[Repositories<br/>database/repositories/]
    end

    subgraph "Web API"
        WEBAPI[FastAPI Backend<br/>web/backend/]
        ROUTERS[Routers<br/>pipeline · matches · notifications]
    end

    subgraph "Notification"
        NOTIF[Notification Service<br/>notification/]
        NOTIF_CHANNELS[Channels<br/>Discord · Email · in-app]
        RQ_WORKER[RQ Worker<br/>Redis Queue]
    end

    subgraph "Monolith Mode"
        MAIN[main.py<br/>sequential pipeline]
    end

    %% Scraping
    SCRAPER --> ORCH
    ORCH -->|extraction:jobs stream| EXTR
    EXTR -->|embeddings:jobs stream| EMBD
    EMBD -->|matching:jobs stream| SCMATCH

    %% Core domain wiring
    ORCH --> ETL
    EXTR --> ETL
    EMBD --> ETL
    SCMATCH --> MATCHER
    SCMATCH --> SCORER
    ETL --> RESUME
    ETL --> LLM
    MATCHER --> LLM
    SCORER --> LLM

    %% Data access
    ETL --> REPOS
    MATCHER --> REPOS
    SCORER --> REPOS
    REPOS --> PG
    ORCH --> REDIS
    EXTR --> REDIS
    EMBD --> REDIS
    SCMATCH --> REDIS

    %% Web API
    WEBAPI --> ROUTERS
    ROUTERS --> REPOS
    ROUTERS --> ORCH

    %% Notifications
    SCORER --> NOTIF
    NOTIF --> RQ_WORKER
    RQ_WORKER --> NOTIF_CHANNELS
    RQ_WORKER --> REDIS

    %% Monolith
    MAIN --> ETL
    MAIN --> MATCHER
    MAIN --> SCORER
    MAIN --> PG
```

---

## Key Execution Flows

### 1. Microservice Startup → Redis Connection (7 steps)

**Trigger**: `docker compose up` starts the Orchestrator service.

```
services/orchestrator/main.py  lifespan()
  └─ services/orchestrator/main.py  _log_stream_backlogs_periodically()
       └─ core/redis_streams.py  log_stream_backlogs()
            └─ core/redis_streams.py  get_all_stream_backlogs()
                 └─ core/redis_streams.py  get_stream_backlog()
                      └─ core/redis_streams.py  get_redis_client()
                           └─ core/redis_streams.py  _get_connection_pool()  ← singleton pool
```

On startup each microservice calls `init_db()` (creates missing tables via `Base.metadata.create_all`) then begins logging Redis stream backlog metrics periodically. The connection pool is a module-level singleton created once and shared across all calls.

---

### 2. Monolith Pipeline → Database Commit (6 steps)

**Trigger**: `python main.py` or scheduled run.

```
main.py  main()
  └─ main.py  run_internal_sequential_cycle()
       └─ main.py  _run_job_etl_phase()
            └─ main.py  run_job_etl()
                 └─ database/uow.py  job_uow()          ← Unit of Work context manager
                      └─ database/repository.py  commit()
```

The monolith runs scrape → ETL → match → score sequentially. Each ETL phase is wrapped in a `job_uow()` Unit of Work that provides a scoped session and auto-commits or rolls back on error.

---

### 3. Resume ETL → LLM Extraction (6 steps)

**Trigger**: Extraction Service receives a message on the `extraction:jobs` Redis Stream.

```
services/base/extraction.py  extract_resume()
  └─ etl/orchestrator.py  extract_resume()
       └─ etl/orchestrator.py  _extract_resume_data()
            └─ etl/resume/profiler.py  extract_only()
                 └─ etl/resume/profiler.py  extract_structured_resume()
                      └─ core/llm/interfaces.py  extract_resume_data()  ← OpenAI call
```

Resume text flows from the service boundary through the ETL orchestrator into the `ResumeProfiler`, which calls the `LLMProvider` interface. The interface is implemented by `OpenAIService`, keeping the domain layer decoupled from the specific LLM vendor.

---

### 4. Scoring Pipeline → Experience Penalty (6 steps)

**Trigger**: `score_matches()` called after preliminary vector matches are found.

```
core/scorer/service.py  score_matches()
  └─ core/scorer/service.py  score_preliminary_match()
       └─ core/scorer/penalties.py  calculate_fit_penalties()
            └─ core/scorer/penalties.py  _calculate_experience_penalty()
                 └─ core/scorer/penalties.py  _calculate_best_experience_years()
                      └─ core/scorer/penalties.py  _extract_years_from_evidence()
```

Each preliminary match is scored with a fit score (required/preferred coverage) plus optional want-score (facet alignment). Penalties are applied for experience gaps — the scorer walks resume evidence units to find the best matching years claim before computing the delta against the job's requirement.

---

### 5. Notification Dispatch → Discord Embed (8 steps)

**Trigger**: High-scoring match saved; `notify_batch_complete()` called.

```
notification/service.py  notify_batch_complete()
  └─ notification/service.py  send_notification()
       └─ notification/service.py  process_notification_task()
            └─ notification/service.py  _send_and_record_notification()
                 └─ notification/channels.py  send()
                      └─ notification/message_builder.py  build_batch_embeds()
                           └─ notification/message_builder.py  to_discord_embed()
                                └─ notification/message_builder.py  _get_score_color()
```

Notifications are enqueued into Redis Queue (RQ). The worker dequeues, deduplicates via `NotificationTrackerService`, and dispatches to the appropriate channel (Discord webhook, Email, in-app). Score color coding (green/yellow/red) is applied at render time.

---

## Data Model

```
job_post                    ← core entity; fingerprinted per (tenant, site, title+company)
  ├─ job_post_source        ← one per (site, url); tracks first/last seen
  ├─ job_requirement_unit   ← structured requirements extracted by LLM
  │    └─ job_requirement_unit_embedding  ← 1024-dim vector per requirement
  ├─ job_benefit            ← structured benefits
  ├─ job_facet_embedding    ← per-facet embeddings (remote_flexibility, tech_stack, …)
  └─ job_match              ← scored match against a resume fingerprint
       └─ job_match_requirement  ← per-requirement match detail

resume (user files)
  ├─ structured_resume      ← parsed JSON profile
  ├─ resume_section_embedding
  ├─ resume_evidence_unit_embedding
  └─ user_wants             ← user preference embeddings per facet

tenant / user / user_file   ← multi-tenant auth layer
notification_tracker        ← deduplication state
app_settings                ← runtime feature flags
```

**Stage flags on `job_post`**: each processing stage (`extraction`, `embedding`, `facet`) has its own `status` / `attempts` / `last_error` / `next_retry_at` columns, enabling independent retry logic per stage without blocking other stages.

---

## Redis Streams Topology

| Stream | Producer | Consumer | Purpose |
|--------|----------|----------|---------|
| `extraction:jobs` | Orchestrator | Extraction Service | LLM extraction jobs |
| `extraction:batch` | Orchestrator | Extraction Service | Batch extraction |
| `embeddings:jobs` | Extraction Service | Embeddings Service | Embedding jobs |
| `embeddings:batch` | Extraction Service | Embeddings Service | Batch embedding |
| `matching:jobs` | Embeddings Service | Scorer-Matcher Service | Matching jobs |

Completion events are published to Redis Pub/Sub channels (`extraction:completed`, `embeddings:completed`, `matching:completed`) so the orchestrator can correlate results. Consumer groups with `XCLAIM` and idle-time reclaim handle crash recovery.

---

## Configuration

All tunable parameters live in YAML config files loaded by `core/config_loader.py` (cohesion 100%):

- `MatcherConfig` — similarity threshold, top-k candidates
- `ScorerConfig` — required/preferred weights, remote preference
- `FacetWeights` — per-facet importance multipliers
- `ResultPolicy` — minimum score cutoffs, deduplication rules

---

## Testing Strategy

| Layer | Infrastructure | What it proves |
|-------|---------------|----------------|
| Pure logic tests | Mock | Algorithm correctness (scoring, parsing, serialization) |
| Repository tests | pgvector testcontainer | Column names exist in schema; queries execute; constraints enforced |
| Redis protocol tests | Redis testcontainer | Stream/PEL/pub-sub behavior; consumer group idempotency |
| Integration tests | Session-scoped testcontainers | Full pipeline roundtrip with real DB and Redis |
| Smoke tests (`tests/smoke/`) | Live services | Full stack with running microservices (`SMOKE_TESTS=1`) |
