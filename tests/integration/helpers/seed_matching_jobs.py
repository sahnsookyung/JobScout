"""Deterministic job corpus seeding for E2E tests."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.llm.schema_models import JOB_OFFERINGS_PROFILE_VERSION
from tests.mocks.fake_service import FakeLLMService
from database.models import (
    CandidatePreferences,
    JobMatch,
    JobMatchRequirement,
    JobOfferingsProfile,
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

def _offering_signal(label: str, evidence: str, confidence: float = 0.8) -> dict:
    return {
        "label": label,
        "evidence": evidence,
        "confidence": confidence,
    }

def _offerings_profile(
    *,
    work_arrangement: str,
    tech_labels: list[str],
    culture_labels: list[str] | None = None,
    growth_labels: list[str] | None = None,
    evidence: str,
    confidence: float = 0.8,
) -> dict:
    return {
        "schema_version": JOB_OFFERINGS_PROFILE_VERSION,
        "work_arrangement": work_arrangement,
        "location_timezone": [],
        "visa_sponsorship": False,
        "compensation": [],
        "benefits_perks": [],
        "flexibility": (
            [_offering_signal("remote work", "Remote", 0.85)]
            if work_arrangement == "remote"
            else []
        ),
        "team_culture": [
            _offering_signal(label, label, 0.8)
            for label in (culture_labels or [])
        ],
        "mentorship_growth": [
            _offering_signal(label, label, 0.8)
            for label in (growth_labels or [])
        ],
        "product_domain": [],
        "tech_environment": [
            _offering_signal(label, label, 0.85)
            for label in tech_labels
        ],
        "negative_signals": [],
        "evidence_snippets": [evidence[:200]],
        "confidence": confidence,
    }


def reset_microservices_state(database_url: str) -> None:
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
        session.query(JobOfferingsProfile).delete()
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

        session.add_all(
            [
                JobOfferingsProfile(
                    job_post_id=positive_job.id,
                    profile_json=_offerings_profile(
                        work_arrangement="remote",
                        tech_labels=[
                            "Python",
                            "FastAPI",
                            "backend",
                            "microservices",
                        ],
                        culture_labels=["modern engineering"],
                        growth_labels=["mentorship"],
                        evidence=positive_job.description,
                    ),
                    profile_schema_version=JOB_OFFERINGS_PROFILE_VERSION,
                    source_description_hash=positive_job.content_hash,
                    extraction_provider="e2e_seed",
                    extraction_model="deterministic",
                    confidence=0.8,
                ),
                JobOfferingsProfile(
                    job_post_id=negative_job.id,
                    profile_json=_offerings_profile(
                        work_arrangement="onsite",
                        tech_labels=["Java", "Spring", "Salesforce"],
                        evidence=negative_job.description,
                        confidence=0.7,
                    ),
                    profile_schema_version=JOB_OFFERINGS_PROFILE_VERSION,
                    source_description_hash=negative_job.content_hash,
                    extraction_provider="e2e_seed",
                    extraction_model="deterministic",
                    confidence=0.7,
                ),
            ]
        )

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
