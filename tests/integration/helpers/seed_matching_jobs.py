"""Deterministic job corpus seeding for split-stack E2E tests."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from tests.mocks.fake_service import FakeLLMService
from database.models import (
    CandidatePreferences,
    JobMatch,
    JobMatchRequirement,
    JobPost,
    JobRequirementUnit,
    JobRequirementUnitEmbedding,
    MatchSelectionItem,
    MatchSelectionRun,
    ResumeEvidenceUnitEmbedding,
    ResumeProcessingState,
    ResumeSectionEmbedding,
    ResumeUpload,
    StructuredResume,
)


@dataclass(frozen=True)
class SeededJobCorpus:
    positive_job_id: str
    negative_job_id: str


def reset_split_stack_state(database_url: str) -> None:
    """Clear matching and resume state so each E2E case starts isolated."""
    engine = create_engine(database_url)
    session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = session_local()
    try:
        session.query(CandidatePreferences).delete()
        session.query(MatchSelectionItem).delete()
        session.query(MatchSelectionRun).delete()
        session.query(JobMatchRequirement).delete()
        session.query(JobMatch).delete()
        session.query(JobRequirementUnitEmbedding).delete()
        session.query(JobRequirementUnit).delete()
        session.query(ResumeEvidenceUnitEmbedding).delete()
        session.query(ResumeSectionEmbedding).delete()
        session.query(ResumeProcessingState).delete()
        session.query(StructuredResume).delete()
        session.query(ResumeUpload).delete()
        session.query(JobPost).delete()
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        engine.dispose()


def seed_matcher_ready_jobs(database_url: str) -> SeededJobCorpus:
    """Insert a tiny matcher-ready corpus in the deterministic fake vector space."""
    engine = create_engine(database_url)
    session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    ai = FakeLLMService()
    session = session_local()
    try:
        positive_job = JobPost(
            title="Senior Python Backend Engineer",
            company="Acme Cloud",
            location_text="Remote",
            is_remote=True,
            canonical_fingerprint="e2e-positive-job",
            fingerprint_version=1,
            status="active",
            is_extracted=True,
            is_embedded=True,
            extraction_status="succeeded",
            embedding_status="succeeded",
            description=(
                "Senior backend engineer working on Python FastAPI APIs, AWS, Docker, "
                "Redis, PostgreSQL, and microservices."
            ),
            content_hash="e2e-positive-hash",
            summary_embedding=ai.generate_embedding(
                "python fastapi aws docker microservices backend api remote"
            ),
        )
        session.add(positive_job)
        session.flush()

        negative_job = JobPost(
            title="Enterprise Java Developer",
            company="Legacy Systems Inc",
            location_text="On-site",
            is_remote=False,
            canonical_fingerprint="e2e-negative-job",
            fingerprint_version=1,
            status="active",
            is_extracted=True,
            is_embedded=True,
            extraction_status="succeeded",
            embedding_status="succeeded",
            description=(
                "On-site Java Spring role focused on Salesforce integrations and "
                "enterprise support."
            ),
            content_hash="e2e-negative-hash",
            summary_embedding=ai.generate_embedding(
                "java spring salesforce enterprise onsite support"
            ),
        )
        session.add(negative_job)
        session.flush()

        positive_requirements = [
            ("must_have", "Experience with python", ["python"]),
            ("must_have", "Experience with fastapi", ["fastapi"]),
            ("must_have", "Experience with aws", ["aws"]),
            ("must_have", "Experience with docker", ["docker"]),
        ]
        negative_requirements = [
            ("must_have", "Experience with java", ["java"]),
            ("must_have", "Experience with spring", ["spring"]),
            ("must_have", "Experience with salesforce", ["salesforce"]),
        ]

        for ordinal, (req_type, text, skills) in enumerate(positive_requirements):
            requirement = JobRequirementUnit(
                job_post_id=positive_job.id,
                req_type=req_type,
                text=text,
                tags={"related_skills": skills},
                ordinal=ordinal,
            )
            session.add(requirement)
            session.flush()
            session.add(
                JobRequirementUnitEmbedding(
                    job_requirement_unit_id=requirement.id,
                    embedding=ai.generate_embedding(text),
                )
            )

        for ordinal, (req_type, text, skills) in enumerate(negative_requirements):
            requirement = JobRequirementUnit(
                job_post_id=negative_job.id,
                req_type=req_type,
                text=text,
                tags={"related_skills": skills},
                ordinal=ordinal,
            )
            session.add(requirement)
            session.flush()
            session.add(
                JobRequirementUnitEmbedding(
                    job_requirement_unit_id=requirement.id,
                    embedding=ai.generate_embedding(text),
                )
            )

        session.commit()
        return SeededJobCorpus(
            positive_job_id=str(positive_job.id),
            negative_job_id=str(negative_job.id),
        )
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        engine.dispose()
