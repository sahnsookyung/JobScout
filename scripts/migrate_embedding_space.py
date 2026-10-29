"""Resumable maintenance backfill for a configured query/passage embedding space.

Dry run by default. Use a maintenance database role via EMBEDDING_MIGRATION_DATABASE_URL.
Take a verified database backup and stop old embedding writers before --apply.
No credentials, source text, or provider response bodies are written to output.
"""

import argparse
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List

from sqlalchemy import create_engine, func, or_, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, selectinload

from core.config_loader import load_config
from core.embedding_contract import current_embedding_space, embedding_space_for_config
from core.llm.global_budget import global_llm_budget_lane
from core.llm.provider_factory import RuntimeLLMConfig, build_llm_provider, runtime_llm_config_from_etl
from core.llm.schema_models import ResumeSchema
from database.models import JobPost, JobRequirementUnit, JobRequirementUnitEmbedding, StructuredResume, RESUME_FINGERPRINT_VERSION
from database.repositories.embedding_contract import job_embeddings_ready_clause
from database.repositories.job_post import JobPostRepository
from database.repositories.resume import ResumeRepository
from etl.resume.profiler import ResumeProfiler
from services.base.embeddings import _build_job_embedding_text


@dataclass(frozen=True)
class BackfillItem:
    id: Any
    text: str


def close_provider(provider: Any) -> None:
    base = getattr(provider, "provider", provider)
    embedding_client = getattr(base, "embedding_client", None)
    if embedding_client is not None:
        embedding_client.close()
    base.client.close()


def embed_batch(runtime: RuntimeLLMConfig, items: List[BackfillItem], kind: str) -> List[List[float]]:
    provider = build_llm_provider(runtime)
    try:
        with global_llm_budget_lane("background"):
            values = [item.text for item in items]
            if kind == "requirements":
                return provider.generate_query_embeddings_batch(values)
            return provider.generate_embeddings_batch(values)
    finally:
        close_provider(provider)


def pending_statement(kind: str, include_inactive: bool = False) -> Any:
    space = current_embedding_space()
    if kind == "requirements":
        statement = (
            select(JobRequirementUnit)
            .join(JobPost, JobPost.id == JobRequirementUnit.job_post_id)
            .outerjoin(JobRequirementUnitEmbedding, JobRequirementUnitEmbedding.job_requirement_unit_id == JobRequirementUnit.id)
            .where(or_(JobRequirementUnitEmbedding.job_requirement_unit_id.is_(None),
                       JobRequirementUnitEmbedding.embedding_space != space))
        )
    else:
        statement = select(JobPost).where(or_(JobPost.summary_embedding.is_(None), JobPost.embedding_space != space))
    statement = statement.where(JobPost.is_extracted.is_(True))
    if not include_inactive:
        statement = statement.where(JobPost.status == "active")
    return statement


def collect_items(engine: Engine, kind: str, limit: int, include_inactive: bool = False) -> List[BackfillItem]:
    statement = pending_statement(kind, include_inactive)
    if kind == "jobs":
        statement = statement.options(selectinload(JobPost.requirements), selectinload(JobPost.benefits)).order_by(JobPost.id)
    else:
        statement = statement.order_by(JobRequirementUnit.id)
    with Session(engine) as session:
        rows = session.execute(statement.limit(limit)).scalars().all()
        return [BackfillItem(row.id, _build_job_embedding_text(row) if kind == "jobs" else row.text) for row in rows]


def persist_items(engine: Engine, kind: str, items: List[BackfillItem], vectors: List[List[float]]) -> Dict[str, int]:
    if len(items) != len(vectors):
        raise ValueError("Backfill response count mismatch")
    snapshots = {item.id: (item, vector) for item, vector in zip(items, vectors, strict=True)}
    model = JobPost if kind == "jobs" else JobRequirementUnit
    statement = select(model).where(model.id.in_(snapshots)).with_for_update()
    if kind == "jobs":
        statement = statement.options(selectinload(JobPost.requirements), selectinload(JobPost.benefits))
    persisted = 0
    with Session(engine) as session, session.begin():
        rows = session.execute(statement).scalars().all()
        requirement_values = []
        for row in rows:
            item, vector = snapshots[row.id]
            current_text = _build_job_embedding_text(row) if kind == "jobs" else row.text
            # The HTTP call runs outside the transaction. Never label changed text
            # with the vector produced from an older snapshot.
            if current_text != item.text:
                continue
            if kind == "jobs":
                JobPostRepository(session).save_job_embedding(row, vector)
            else:
                requirement_values.append({"job_requirement_unit_id": row.id, "embedding": vector,
                                           "embedding_space": current_embedding_space()})
            persisted += 1
        if requirement_values:
            insert = pg_insert(JobRequirementUnitEmbedding).values(requirement_values)
            session.execute(insert.on_conflict_do_update(
                index_elements=[JobRequirementUnitEmbedding.job_requirement_unit_id],
                set_={"embedding": insert.excluded.embedding, "embedding_space": insert.excluded.embedding_space},
            ))
    return {"persisted": persisted, "changed_or_deleted": len(items)-persisted}


def backfill_catalog(engine: Engine, runtime: RuntimeLLMConfig, kind: str, *,
                     workers: int = 2, max_batches: int = 0, include_inactive: bool = False) -> int:
    completed = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        while not max_batches or completed < max_batches:
            slots = workers if not max_batches else min(workers, max_batches-completed)
            items = collect_items(engine, kind, slots*32, include_inactive)
            if not items:
                break
            futures = {executor.submit(embed_batch, runtime, items[i:i+32], kind): items[i:i+32]
                       for i in range(0, len(items), 32)}
            failed = False
            for future in as_completed(futures):
                try:
                    result = persist_items(engine, kind, futures[future], future.result())
                    completed += 1
                    print(json.dumps({"stage": kind, "batches": completed, **result}), flush=True)
                except Exception as exc:
                    failed = True
                    print(json.dumps({"stage": kind, "error_type": type(exc).__name__,
                                      "http_status": getattr(exc, "status_code", None)}), flush=True)
            if failed:
                raise RuntimeError("Backfill stopped after a failed batch; successful batches remain checkpointed")
    return completed


def backfill_resumes(engine: Engine, runtime: RuntimeLLMConfig) -> int:
    # Resume calls stay in the ordinary interactive budget lane. They do not use
    # the public-catalog budget exemption.
    with Session(engine) as session:
        fingerprints = list(session.execute(select(StructuredResume.resume_fingerprint).where(
            StructuredResume.fingerprint_version == RESUME_FINGERPRINT_VERSION)).scalars())
    completed = 0
    for fingerprint in fingerprints:
        with Session(engine) as session:
            repo = ResumeRepository(session)
            if repo.is_resume_ready(fingerprint):
                continue
            row = repo.get_structured_resume_by_fingerprint(fingerprint)
            if row is None:
                continue
            source = dict(row.extracted_data)
            source_updated_at = row.updated_at
            owner_id = row.owner_id
        resume = ResumeSchema.model_validate(source)
        provider = build_llm_provider(runtime)
        try:
            with global_llm_budget_lane("interactive"):
                profiler = ResumeProfiler(provider)
                units = profiler.extract_resume_evidence(resume.profile)
                profiler.embed_evidence_units(units)
                sections = profiler.save_resume_section_embeddings(fingerprint, resume, owner_id=owner_id)
        finally:
            close_provider(provider)
        evidence = [{"evidence_unit_id": unit.id, "source_text": unit.text, "source_section": unit.source_section,
                     "tags": unit.tags, "embedding": unit.embedding, "years_value": unit.years_value,
                     "years_context": unit.years_context, "is_total_years_claim": unit.is_total_years_claim}
                    for unit in units]
        if not evidence or not any(s['section_type'] == 'summary' and s.get('query_embedding') is not None for s in sections):
            raise ValueError("Resume is missing evidence or a retrieval query vector")
        with Session(engine) as session, session.begin():
            current = session.execute(select(StructuredResume).where(
                StructuredResume.resume_fingerprint == fingerprint).with_for_update()).scalar_one_or_none()
            if current is None or current.updated_at != source_updated_at or current.owner_id != owner_id:
                raise RuntimeError("Resume changed during backfill; rerun to use its current contents")
            repo = ResumeRepository(session)
            repo.save_resume_section_embeddings(fingerprint, sections, owner_id=owner_id)
            repo.save_evidence_unit_embeddings(fingerprint, evidence, owner_id=owner_id)
            repo.set_resume_processing_state(fingerprint, "ready", owner_id=owner_id,
                                            embedding_completed_at=datetime.now(timezone.utc))
        completed += 1
        print(json.dumps({"stage": "resumes", "completed": completed}), flush=True)
    return completed


def migration_status(engine: Engine, include_inactive: bool = False) -> Dict[str, Any]:
    with Session(engine) as session:
        counts = {kind: session.scalar(select(func.count()).select_from(pending_statement(kind, include_inactive).subquery()))
                  for kind in ("jobs", "requirements")}
        ready = select(func.count(JobPost.id)).where(JobPost.is_extracted.is_(True),
            JobPost.is_embedded.is_(True), JobPost.summary_embedding.isnot(None), job_embeddings_ready_clause())
        if not include_inactive:
            ready = ready.where(JobPost.status == "active")
        counts["ready_jobs"] = session.scalar(ready)
        fingerprints = list(session.execute(select(StructuredResume.resume_fingerprint).where(
            StructuredResume.fingerprint_version == RESUME_FINGERPRINT_VERSION)).scalars())
        counts["resumes_pending"] = sum(not ResumeRepository(session).is_resume_ready(fp) for fp in fingerprints)
        counts["outdated_resumes"] = session.scalar(select(func.count(StructuredResume.id)).where(
            StructuredResume.fingerprint_version != RESUME_FINGERPRINT_VERSION))
        return {"embedding_space": current_embedding_space(), **counts}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--include-resumes", action="store_true", help="Explicitly include stored resume evidence in provider calls")
    parser.add_argument("--include-inactive", action="store_true")
    parser.add_argument("--stage", choices=["all", "resumes", "requirements", "jobs"], default="all")
    parser.add_argument("--workers", type=int, choices=[1, 2], default=2)
    parser.add_argument("--max-batches", type=int, default=0)
    args = parser.parse_args()
    if args.max_batches < 0:
        parser.error("--max-batches must be nonnegative")
    database_url = os.environ.get("EMBEDDING_MIGRATION_DATABASE_URL")
    if not database_url:
        parser.error("Set EMBEDDING_MIGRATION_DATABASE_URL to a maintenance database connection")
    config = load_config().etl.llm
    if not config.embedding_input_type_enabled or embedding_space_for_config(config) != current_embedding_space():
        parser.error("Configure a consistent query/passage embedding space before backfilling")
    runtime = runtime_llm_config_from_etl(config).model_copy(update={"retry_max_attempts": 1})
    engine = create_engine(database_url, pool_size=3, max_overflow=0)
    try:
        with engine.connect() as connection:
            if not connection.scalar(text("SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user")):
                raise PermissionError("Maintenance role required; do not change application RLS permissions")
        print(json.dumps({"before": migration_status(engine, args.include_inactive)}), flush=True)
        if not args.apply:
            return 0
        if args.stage in {"all", "resumes"} and args.include_resumes:
            backfill_resumes(engine, runtime)
        if args.stage == "resumes" and not args.include_resumes:
            parser.error("Resume backfill requires --include-resumes")
        for stage in ("requirements", "jobs"):
            if args.stage in {"all", stage}:
                backfill_catalog(engine, runtime, stage, workers=args.workers,
                                 max_batches=args.max_batches, include_inactive=args.include_inactive)
        status = migration_status(engine, args.include_inactive)
        print(json.dumps({"after": status}), flush=True)
        return 2 if any(status[key] for key in ("jobs", "requirements", "resumes_pending")) else 0
    except Exception as exc:
        print(json.dumps({"error_type": type(exc).__name__, "http_status": getattr(exc, "status_code", None)}), flush=True)
        return 1
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
