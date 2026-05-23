from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple, Optional, cast
import logging
import os
import math
from database.repository import JobRepository
from core.llm.interfaces import LLMProvider
from core.utils import JobFingerprinter
from pydantic import ValidationError
from core.llm.schema_models import JobExtraction, ResumeSchema
from database.models import (
    RESUME_PROCESSING_EMBEDDING,
    RESUME_PROCESSING_EXTRACTED,
    RESUME_PROCESSING_EXTRACTING,
    RESUME_PROCESSING_FAILED,
    RESUME_PROCESSING_READY,
    generate_file_fingerprint,
)
from etl.canonical_summary import CanonicalJobSummaryGenerator
from etl.import_models import NormalizedJobRecord
from etl.resume import ResumeProfiler, ResumeParser
from etl.resume.embedding_store import JobRepositoryAdapter

logger = logging.getLogger(__name__)
SYSTEM_OWNER_ID = "00000000-0000-0000-0000-000000000001"
FAILED_PARSE_RESUME_FILE = "Failed to parse resume file"
def _effective_owner_id(owner_id: Optional[Any]) -> Any:
    return owner_id or SYSTEM_OWNER_ID


def _validate_embedding_vector(vector: List[float], context: str) -> List[float]:
    """Ensure embedding vectors are non-empty finite numeric lists."""
    if not isinstance(vector, list) or not vector:
        raise ValueError(f"Invalid embedding vector for {context}: empty or non-list")

    validated: List[float] = []
    for value in vector:
        if not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            raise ValueError(f"Invalid embedding vector for {context}: non-finite value")
        validated.append(float(value))

    return validated


class JobETLService:
    """Service for processing individual ETL jobs with per-item transactions.

    This service provides per-item processing methods that should be called
    within a job_uow() context manager. The service does not manage transactions
    internally - all commits/rollbacks are handled by the UoW.

    Usage:
        with job_uow() as repo:
            service = JobETLService(ai_service)
            service.ingest_one(repo, job_data, site_name)
        # commit happens automatically
    """

    def __init__(self, ai_service: LLMProvider):
        self.ai = ai_service
        self.canonical_summary_generator = CanonicalJobSummaryGenerator()

    def ingest_one(self, repo: JobRepository, job_data: Dict[str, Any], site_name: str) -> None:
        record = NormalizedJobRecord.from_scraper_payload(job_data, site_name)
        self.import_record(repo, record)

    def import_record(self, repo: JobRepository, record: NormalizedJobRecord) -> None:
        """Ingest a single raw job from scrapers.

        Args:
            repo: JobRepository instance (provided by UoW)
            record: Provider-neutral normalized job record
        """
        job_data = record.as_job_data()
        title = job_data.get('title')
        company = job_data.get('company_name')
        if not title or not company:
            logger.warning("Skipping job with missing title or company")
            return

        # 1. Fingerprint & Normalization
        location_text = JobFingerprinter.normalize_location(job_data.get('location'))
        fingerprint = record.canonical_dedupe_fingerprint()
        source_job_url = str(job_data.get("job_url") or "")

        # 2. Duplicate Check
        source_match = False
        job_post = (
            repo.get_by_source(
                record.source.site_name,
                source_job_url,
                tenant_id=record.tenant_id,
            )
            if source_job_url
            else None
        )
        if job_post:
            source_match = True
            logger.info("Found existing job by source identity: %s", source_job_url)
            repo.update_timestamp(job_post)
        else:
            job_post = repo.get_by_fingerprint(fingerprint, tenant_id=record.tenant_id)

        if not job_post:
            logger.info(f"New job found: {title} at {company}")
            job_post = repo.create_job_post(
                job_data,
                fingerprint,
                location_text,
                tenant_id=record.tenant_id,
            )
        elif not source_match:
            repo.update_timestamp(job_post)

        logger.info(f"Duplicate found for {title}. ID: {job_post.id}")

        # 3. Create Source & Content
        repo.get_or_create_source(
            job_post.id,
            record.source.site_name,
            job_data,
            tenant_id=record.tenant_id,
        )
        repo.save_job_content(job_post.id, job_data)

    def extract_one(self, repo: JobRepository, job) -> None:
        """Extract structured data from a single job description.

        Args:
            repo: JobRepository instance (provided by UoW)
            job: JobPost ORM instance (loaded within this UoW session)
        """
        logger.info(f"Extracting for job {job.id}: {job.title}")

        extraction_result = self.ai.extract_requirements_data(job.description)

        # Check if extraction returned meaningful data
        requirements = extraction_result.get('requirements', [])
        if not requirements:
            raise ValueError(f"Empty requirements extraction for job {job.id}")

        # Validate with Pydantic model
        if extraction_result:
            try:
                job_extraction = JobExtraction.model_validate(extraction_result)
                data = job_extraction.model_dump()
            except ValidationError:
                logger.exception("Failed to validate job extraction")
                data = extraction_result
        else:
            data = {}

        canonical_summary = self.canonical_summary_generator.generate(job, data)
        data["canonical_job_summary"] = canonical_summary.text
        data["canonical_job_summary_version"] = canonical_summary.version
        data["canonical_job_summary_hash"] = canonical_summary.content_hash

        repo.update_job_metadata(job, data)
        repo.update_content_metadata(job.id, data)
        repo.save_requirements(job, data.get('requirements', []))
        repo.save_benefits(job, data.get('benefits', []))
        repo.mark_as_extracted(job)

    def embed_job_one(self, repo: JobRepository, job) -> None:
        """Generate embedding for a single job.

        Args:
            repo: JobRepository instance (provided by UoW)
            job: JobPost ORM instance (loaded within this UoW session)
        """
        parts = []
        text = getattr(job, "canonical_job_summary", None) or ""

        if not text and isinstance(getattr(job, "raw_payload", None), dict):
            text = job.raw_payload.get("ai_job_summary", "")

        if not text and job.requirements:
            parts.extend([r.text for r in job.requirements[:20]])

        if not text and job.benefits:
            parts.extend([b.text for b in job.benefits[:10]])

        if not text and parts:
            text = " | ".join(parts)
        elif not text:
            logger.warning(f"Job {job.id} has no requirements/benefits, using description for summary_embedding")
            text = job.description[:5000] if job.description else ""

        vector = _validate_embedding_vector(
            self.ai.generate_embedding(text),
            f"job {job.id}",
        )

        repo.save_job_embedding(job, vector)

    def embed_requirement_one(self, repo: JobRepository, req) -> None:
        """Generate embedding for a single requirement.

        Args:
            repo: JobRepository instance (provided by UoW)
            req: JobRequirementUnit ORM instance (loaded within this UoW session)
        """
        vector = _validate_embedding_vector(
            self.ai.generate_embedding(req.text),
            f"requirement {req.id}",
        )
        repo.save_requirement_embedding(req.id, vector)

    def _load_and_check_resume(
        self,
        repo: JobRepository,
        resume_file: str,
        known_fingerprint: Optional[str] = None
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """Common logic: load resume file, generate fingerprint, check if changed.

        Args:
            repo: JobRepository instance (provided by UoW)
            resume_file: Path to resume file
            known_fingerprint: Optional pre-computed fingerprint from raw file bytes.
                              If provided, skips re-reading file for fingerprint.

        Returns:
            Tuple of (resume_changed: bool, fingerprint: str, resume_data: dict or None)
            - resume_changed: True if resume is new/changed, False if unchanged
            - fingerprint: The resume fingerprint
            - resume_data: The loaded resume data (or None if file not found/error)
        """
        # Use provided fingerprint if available
        if known_fingerprint:
            fingerprint = known_fingerprint
        else:
            # Read file and compute fingerprint
            if not os.path.exists(resume_file):
                logger.error(f"Resume file not found: {resume_file}")
                return False, "", None

            try:
                with open(resume_file, 'rb') as f:
                    file_bytes = f.read()
                fingerprint = generate_file_fingerprint(file_bytes)
            except IOError:
                logger.exception("Failed to read resume file")
                return False, "", None

        logger.info(f"Resume fingerprint: {fingerprint}")

        existing = repo.resume.get_structured_resume_by_fingerprint(fingerprint)
        if existing:
            logger.info(f"Resume unchanged (fingerprint: {fingerprint}), skipping")
            return False, fingerprint, None

        # Parse the resume
        try:
            parser = ResumeParser()
            parsed = parser.parse(resume_file)
            resume_data = parsed.data if parsed.data is not None else {"raw_text": parsed.text}
        except (ValueError, IOError):
            logger.exception(FAILED_PARSE_RESUME_FILE)
            return False, fingerprint, None

        logger.info(f"Resume changed (fingerprint: {fingerprint}), processing...")
        return True, fingerprint, resume_data

    def process_resume(
        self,
        repo: JobRepository,
        resume_file: str,
        force_re_extraction: bool = False,
        *,
        owner_id: Optional[Any] = None,
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """Run resumable extract+embed processing for a resume file."""
        owner_id = _effective_owner_id(owner_id)
        if not os.path.exists(resume_file):
            logger.error(f"Resume file not found: {resume_file}")
            return False, "", None

        try:
            with open(resume_file, 'rb') as handle:
                fingerprint = generate_file_fingerprint(handle.read())
        except IOError:
            logger.exception("Failed to read resume file")
            return False, "", None

        logger.info(f"Resume fingerprint: {fingerprint[:16]}...")

        if repo.is_resume_ready(fingerprint) and not force_re_extraction:
            logger.info(
                "Resume ready (fingerprint: %s...), skipping ETL",
                fingerprint[:16],
            )
            return False, fingerprint, None

        state = repo.get_resume_processing_state(fingerprint)
        if state and state.processing_status in {
            RESUME_PROCESSING_EXTRACTING,
            RESUME_PROCESSING_EMBEDDING,
        } and not force_re_extraction:
            logger.info(
                "Resume already processing (fingerprint: %s..., status=%s)",
                fingerprint[:16],
                state.processing_status,
            )
            return False, fingerprint, None

        try:
            parser = ResumeParser()
            parsed = parser.parse(resume_file)
            resume_data = parsed.data if parsed.data is not None else {"raw_text": parsed.text}
        except (ValueError, IOError) as exc:
            repo.set_resume_processing_state(
                fingerprint,
                RESUME_PROCESSING_FAILED,
                owner_id=owner_id,
                error=str(exc),
            )
            logger.exception(FAILED_PARSE_RESUME_FILE)
            return False, "", None

        if (
            state
            and state.processing_status == RESUME_PROCESSING_EXTRACTED
            and not force_re_extraction
        ):
            logger.info(
                "Resume extracted but not ready (fingerprint: %s...), resuming embedding",
                fingerprint[:16],
            )
            try:
                self.embed_resume_one(
                    repo,
                    fingerprint,
                    owner_id=owner_id,
                )
                logger.info(
                    "Resume embedding completed for fingerprint: %s...",
                    fingerprint[:16],
                )
                return True, fingerprint, resume_data
            except Exception as exc:
                repo.set_resume_processing_state(
                    fingerprint,
                    RESUME_PROCESSING_FAILED,
                    owner_id=owner_id,
                    error=str(exc),
                )
                logger.exception("Failed to resume embedding for resume")
                raise

        if force_re_extraction:
            logger.info(
                "Force re-extraction enabled for fingerprint: %s...",
                fingerprint[:16],
            )
        else:
            logger.info(f"Resume changed (fingerprint: {fingerprint[:16]}...), processing...")

        try:
            repo.set_resume_processing_state(
                fingerprint,
                RESUME_PROCESSING_EXTRACTING,
                owner_id=owner_id,
                error=None,
            )
            self.extract_resume_one(
                repo,
                resume_data,
                fingerprint,
                owner_id=owner_id,
            )
            logger.info(f"Resume ETL completed for fingerprint: {fingerprint[:16]}...")
            return True, fingerprint, resume_data
        except Exception as exc:
            repo.set_resume_processing_state(
                fingerprint,
                RESUME_PROCESSING_FAILED,
                owner_id=owner_id,
                error=str(exc),
            )
            logger.exception("Failed to process resume")
            raise

    def extract_resume_one(
        self,
        repo: JobRepository,
        resume_data: Dict[str, Any],
        fingerprint: str,
        *,
        owner_id: Optional[Any] = None,
    ) -> None:
        """Extract structured resume data, persist it, then start embedding."""
        owner_id = _effective_owner_id(owner_id)
        logger.info("Extracting structured resume data...")

        profiler = ResumeProfiler(ai_service=self.ai)
        resume = profiler.extract_structured_resume(resume_data)
        if not resume:
            raise ValueError("Structured resume extraction failed")

        logger.info(
            "Total experience: %s years",
            resume.claimed_total_years or "unknown",
        )
        repo.save_structured_resume(
            owner_id=owner_id,
            resume_fingerprint=fingerprint,
            extracted_data=resume.model_dump(),
            total_experience_years=resume.claimed_total_years,
            extraction_confidence=resume.extraction.confidence if resume.extraction else None,
            extraction_warnings=resume.extraction.warnings if resume.extraction else [],
        )
        logger.info("Saved structured resume to database")

        repo.set_resume_processing_state(
            fingerprint,
            RESUME_PROCESSING_EXTRACTED,
            owner_id=owner_id,
            error=None,
            extraction_completed_at=datetime.now(timezone.utc),
        )

        self.embed_resume_one(
            repo,
            fingerprint,
            resume,
            owner_id=owner_id,
        )

    def _assert_resume_ready_artifacts(
        self,
        repo: JobRepository,
        fingerprint: str,
        persistence_payload: List[Dict[str, Any]],
        evidence_units: List[Any],
    ) -> None:
        if not persistence_payload:
            raise ValueError("No resume section embeddings were generated")
        if not evidence_units:
            raise ValueError("No resume evidence embeddings were generated")
        if repo.get_resume_summary_embedding(fingerprint) is None:
            raise ValueError("No summary embedding found after resume embedding")
        if not repo.is_resume_ready(fingerprint):
            raise ValueError("Resume artifacts were generated but readiness verification failed")

    def ensure_resume_ready(
        self,
        repo: JobRepository,
        fingerprint: str,
        pre_extracted_resume: Optional[ResumeSchema] = None,
        *,
        owner_id: Optional[Any] = None,
    ) -> None:
        """Generate all resume embeddings and promote the fingerprint to ready."""
        owner_id = _effective_owner_id(owner_id)
        repo.set_resume_processing_state(
            fingerprint,
            RESUME_PROCESSING_EMBEDDING,
            owner_id=owner_id,
            error=None,
        )

        if pre_extracted_resume is None:
            structured = repo.resume.get_structured_resume_by_fingerprint(fingerprint)
            if structured is None or not structured.extracted_data:
                raise ValueError(f"Structured resume missing for fingerprint: {fingerprint}")
            pre_extracted_resume = ResumeSchema.model_validate(structured.extracted_data)

        profiler = ResumeProfiler(
            ai_service=self.ai,
            store=JobRepositoryAdapter(repo),
        )
        _, evidence_units, persistence_payload = profiler.profile_resume(
            {},
            resume_fingerprint=fingerprint,
            pre_extracted_resume=pre_extracted_resume,
            owner_id=owner_id,
        )

        repo.set_resume_processing_state(
            fingerprint,
            RESUME_PROCESSING_READY,
            owner_id=owner_id,
            error=None,
            embedding_completed_at=datetime.now(timezone.utc),
        )
        self._assert_resume_ready_artifacts(
            repo,
            fingerprint,
            persistence_payload,
            evidence_units,
        )

    def embed_resume_one(
        self,
        repo: JobRepository,
        fingerprint: str,
        pre_extracted_resume: Optional[ResumeSchema] = None,
        *,
        owner_id: Optional[Any] = None,
    ) -> None:
        """Backward-compatible alias for lifecycle-aware embedding."""
        self.ensure_resume_ready(
            repo,
            fingerprint,
            pre_extracted_resume,
            owner_id=owner_id,
        )

    def extract_and_embed_resume(
        self,
        repo: JobRepository,
        resume_file: str,
        known_fingerprint: Optional[str] = None,
        on_progress: Optional[Any] = None,
        *,
        owner_id: Optional[Any] = None,
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """Full ETL: extract structured data AND generate embeddings.

        This method implements the complete resume ETL pipeline with fingerprint-based
        change detection. It checks if the resume has changed and only processes
        if needed.

        Args:
            repo: JobRepository instance (provided by UoW)
            resume_file: Path to resume JSON file
            known_fingerprint: Optional pre-computed fingerprint from raw file bytes.
                              If provided, skips re-reading file for fingerprint.

        Returns:
            Tuple of (resume_changed: bool, fingerprint: str, resume_data: dict or None)
            - resume_changed: True if resume was processed (changed), False if unchanged
            - fingerprint: The resume fingerprint
            - resume_data: The loaded resume data (or None if file not found)
        """
        changed, fingerprint, resume_data = self._load_and_check_resume(
            repo, resume_file, known_fingerprint
        )
        if not changed:
            return False, fingerprint, None

        try:
            extracted, fingerprint, _ = self._extract_resume_data(
                repo,
                fingerprint,
                cast(Dict[str, Any], resume_data),
                owner_id=owner_id,
            )
            if not extracted:
                return False, fingerprint, None

            if on_progress:
                on_progress("embedding")
            self.embed_resume(repo, fingerprint, owner_id=owner_id)
            logger.info(f"Resume ETL completed for fingerprint: {fingerprint}")
            return True, fingerprint, resume_data
        except Exception:
            logger.exception("Failed to process resume")
            raise

    def _extract_resume_data(
        self,
        repo: JobRepository,
        fingerprint: str,
        resume_data: Dict[str, Any],
        *,
        owner_id: Optional[Any] = None,
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """Extract structured resume data from already-parsed data (no embeddings).

        This is an internal helper that performs extraction without re-checking
        fingerprint or re-parsing the file.

        Args:
            repo: JobRepository instance (provided by UoW)
            fingerprint: Resume fingerprint
            resume_data: Already-parsed resume data

        Returns:
            Tuple of (extracted: bool, fingerprint: str, resume_data: dict or None)
        """
        try:
            owner_id = _effective_owner_id(owner_id)
            profiler = ResumeProfiler(ai_service=self.ai)
            resume_schema = profiler.extract_only(resume_data)

            if resume_schema:
                logger.info(f"Resume extraction completed for fingerprint: {fingerprint}")

                repo.save_structured_resume(
                    owner_id=owner_id,
                    resume_fingerprint=fingerprint,
                    extracted_data=resume_schema.model_dump(),
                    total_experience_years=resume_schema.claimed_total_years,
                    extraction_confidence=resume_schema.extraction.confidence if resume_schema.extraction else None,
                    extraction_warnings=resume_schema.extraction.warnings if resume_schema.extraction else []
                )
                logger.info("Saved structured resume to database")
                repo.set_resume_processing_state(
                    fingerprint,
                    RESUME_PROCESSING_EXTRACTED,
                    owner_id=owner_id,
                    error=None,
                    extraction_completed_at=datetime.now(timezone.utc),
                )

                return True, fingerprint, resume_data
            else:
                logger.error("Failed to extract schema from resume")
                return False, fingerprint, None
        except Exception:
            logger.exception("Failed to extract resume")
            raise

    def extract_resume(
        self,
        repo: JobRepository,
        resume_file: str,
        known_fingerprint: Optional[str] = None,
        *,
        owner_id: Optional[Any] = None,
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """Extract structured resume data (no embeddings).

        This method reads the resume file, generates fingerprint, extracts
        structured data, and saves to DB. Does NOT generate embeddings.

        Args:
            repo: JobRepository instance (provided by UoW)
            resume_file: Path to resume file
            known_fingerprint: Optional pre-computed fingerprint from raw file bytes.
                              If provided, skips re-reading file for fingerprint.

        Returns:
            Tuple of (extracted: bool, fingerprint: str, resume_data: dict or None)
        """
        # If fingerprint is already known, skip file reading and use provided value
        owner_id = _effective_owner_id(owner_id)
        if known_fingerprint:
            fingerprint = known_fingerprint
            # Check if already processed
            existing = repo.resume.get_structured_resume_by_fingerprint(fingerprint)
            if existing:
                logger.info(f"Resume unchanged (fingerprint: {fingerprint}), skipping")
                return False, fingerprint, None

            # Parse the resume (file still needs to be read for parsing)
            try:
                parser = ResumeParser()
                parsed = parser.parse(resume_file)
                resume_data = parsed.data if parsed.data is not None else {"raw_text": parsed.text}
            except (ValueError, IOError) as e:
                repo.set_resume_processing_state(
                    fingerprint,
                    RESUME_PROCESSING_FAILED,
                    owner_id=owner_id,
                    error=str(e),
                )
                logger.exception(FAILED_PARSE_RESUME_FILE)
                return False, fingerprint, None

            logger.info(f"Resume changed (fingerprint: {fingerprint}), processing...")
        else:
            # Full flow: load, check fingerprint, parse
            changed, fingerprint, resume_data = self._load_and_check_resume(repo, resume_file)
            if not changed:
                return False, fingerprint, None

        repo.set_resume_processing_state(
            fingerprint,
            RESUME_PROCESSING_EXTRACTING,
            owner_id=owner_id,
            error=None,
        )
        return self._extract_resume_data(
            repo,
            fingerprint,
            cast(Dict[str, Any], resume_data),
            owner_id=owner_id,
        )

    def embed_resume(
        self,
        repo: JobRepository,
        resume_fingerprint: str,
        *,
        owner_id: Optional[Any] = None,
    ) -> Tuple[bool, str]:
        """Generate embeddings for already-extracted resume.

        This method reads the structured resume from DB, generates embeddings,
        and saves them to DB. Requires extraction to have already been done.

        Args:
            repo: JobRepository instance (provided by UoW)
            resume_fingerprint: Resume fingerprint

        Returns:
            Tuple of (embedded: bool, fingerprint: str)
        """
        owner_id = _effective_owner_id(owner_id)
        existing = repo.resume.get_structured_resume_by_fingerprint(resume_fingerprint)
        if not existing:
            logger.error(f"Resume not found in DB: {resume_fingerprint}")
            return False, resume_fingerprint

        if not existing.extracted_data:
            logger.error(f"No extracted data for resume: {resume_fingerprint}")
            return False, resume_fingerprint

        logger.info(f"Generating embeddings for resume: {resume_fingerprint}")

        try:
            resume = ResumeSchema.model_validate(existing.extracted_data)
            self.ensure_resume_ready(
                repo,
                resume_fingerprint,
                pre_extracted_resume=resume,
                owner_id=owner_id,
            )
            logger.info(f"Resume embeddings completed for fingerprint: {resume_fingerprint}")
            return True, resume_fingerprint
        except Exception as e:
            repo.set_resume_processing_state(
                resume_fingerprint,
                RESUME_PROCESSING_FAILED,
                owner_id=owner_id,
                error=str(e),
            )
            logger.exception("Failed to embed resume")
            raise

    def extract_resume_stage(
        self,
        repo: JobRepository,
        resume_file: str,
        known_fingerprint: Optional[str] = None,
        *,
        owner_id: Optional[Any] = None,
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """Named stage wrapper for microservice extraction."""
        return self.extract_resume(
            repo,
            resume_file,
            known_fingerprint,
            owner_id=owner_id,
        )

    def embed_resume_stage(
        self,
        repo: JobRepository,
        resume_fingerprint: str,
        *,
        owner_id: Optional[Any] = None,
    ) -> Tuple[bool, str]:
        """Named stage wrapper for microservice embedding."""
        return self.embed_resume(
            repo,
            resume_fingerprint,
            owner_id=owner_id,
        )

    def unload_models(self):
        """Helper to unload models if the provider supports it."""
        if hasattr(self.ai, 'unload_model'):
            self.ai.unload_model(self.ai.extraction_model)
            self.ai.unload_model(self.ai.embedding_model)
