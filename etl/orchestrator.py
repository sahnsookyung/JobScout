from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple, Optional, cast
import time
import logging
import json
import os
import math
from database.repository import JobRepository
from core.llm.interfaces import LLMProvider
from core.utils import JobFingerprinter
from pydantic import ValidationError
from core.llm.schema_models import JobExtraction, ResumeSchema
from core.scorer.want_score import FACET_KEYS
from database.models import (
    RESUME_PROCESSING_EMBEDDING,
    RESUME_PROCESSING_EXTRACTED,
    RESUME_PROCESSING_EXTRACTING,
    RESUME_PROCESSING_FAILED,
    RESUME_PROCESSING_READY,
    generate_file_fingerprint,
)
from etl.resume import ResumeProfiler, ResumeParser
from etl.resume.embedding_store import JobRepositoryAdapter

logger = logging.getLogger(__name__)
DEFAULT_LEGACY_OWNER_ID = "00000000-0000-0000-0000-000000000001"


def _effective_owner_id(owner_id: Optional[Any]) -> Any:
    return owner_id or DEFAULT_LEGACY_OWNER_ID


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

    def ingest_one(self, repo: JobRepository, job_data: Dict[str, Any], site_name: str) -> None:
        """Ingest a single raw job from scrapers.

        Args:
            repo: JobRepository instance (provided by UoW)
            job_data: Raw job data from scraper
            site_name: Name of the site that scraped the job
        """
        title = job_data.get('title')
        company = job_data.get('company_name')
        if not title or not company:
            logger.warning("Skipping job with missing title or company")
            return

        # 1. Fingerprint & Normalization
        location_text = JobFingerprinter.normalize_location(job_data.get('location'))
        fingerprint = JobFingerprinter.calculate(company, title, location_text)

        # 2. Duplicate Check
        job_post = repo.get_by_fingerprint(fingerprint)
        if job_post:
            logger.info(f"Duplicate found for {title}. ID: {job_post.id}")
            repo.update_timestamp(job_post)
        else:
            logger.info(f"New job found: {title} at {company}")
            job_post = repo.create_job_post(job_data, fingerprint, location_text)

        # 3. Create Source & Content
        repo.get_or_create_source(job_post.id, site_name, job_data)
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
            except ValidationError as e:
                logger.error(f"Failed to validate job extraction: {e}")
                data = extraction_result
        else:
            data = {}

        repo.update_job_metadata(job, data)
        repo.update_content_metadata(job.id, data)
        repo.save_requirements(job, data.get('requirements', []))
        repo.save_benefits(job, data.get('benefits', []))
        repo.mark_as_extracted(job)

    def extract_facets_one(self, repo: JobRepository, job) -> None:
        """Extract job facets for a single job for Want score matching.

        Extracts per-facet text from job descriptions. Embedding is done
        separately in embed_facets_one() for better batch efficiency.

        Args:
            repo: JobRepository instance (provided by UoW)
            job: JobPost ORM instance (loaded within this UoW session)
        """
        logger.info(f"Extracting facets for job {job.id}: {job.title}")

        try:
            repo.delete_all_facet_embeddings_for_job(job.id)

            facets = self.ai.extract_facet_data(job.description)

            content_hash = job.content_hash or ''
            saved_count = 0

            for facet_key in FACET_KEYS:
                facet_text = facets.get(facet_key, "")
                if facet_text:
                    repo.save_job_facet_embedding(
                        job.id, facet_key, facet_text, None, content_hash
                    )
                    saved_count += 1
                else:
                    logger.debug(f"Empty facet '{facet_key}' for job {job.id}")

            logger.info(f"Saved {saved_count} facets for job {job.id}")

            repo.mark_job_facets_extracted(job.id, content_hash)

        except Exception as e:
            repo.mark_job_facets_failed(job.id, str(e))
            logger.error(f"Facet extraction failed for job {job.id}: {e}")
            raise

    def embed_facets_one(self, repo: JobRepository, job) -> int:
        """Generate embeddings for extracted facets of a single job.

        Args:
            repo: JobRepository instance (provided by UoW)
            job: JobPost ORM instance (loaded within this UoW session)

        Returns:
            Number of new facet embeddings created (0 if all already embedded).
        """
        try:
            facets = repo.get_facets_for_job(job.id)
            if not facets:
                logger.debug(f"No facets found for job {job.id}")
                return 0

            unembedded = [f for f in facets if f.embedding is None]
            if not unembedded:
                logger.debug(
                    f"All {len(facets)} facets already embedded for job {job.id} — skipping"
                )
                return 0

            logger.info(
                f"Embedding {len(unembedded)}/{len(facets)} facets for job {job.id}: {job.title}"
            )
            content_hash = job.content_hash or ''
            saved_count = 0

            for facet in unembedded:
                # DB model stores facet content in facet_text; keep text fallback for older mocks.
                facet_text = getattr(facet, "facet_text", None) or getattr(facet, "text", None)
                if not facet_text:
                    logger.debug(f"Facet {getattr(facet, 'id', 'unknown')} has no text, skipping")
                    continue
                embedding = _validate_embedding_vector(
                    self.ai.generate_embedding(facet_text),
                    f"job facet {facet.id}",
                )
                repo.update_facet_embedding(facet.id, embedding, content_hash)
                saved_count += 1

            return saved_count

        except Exception as e:
            logger.error(f"Facet embedding failed for job {job.id}: {e}")
            raise

    def embed_job_one(self, repo: JobRepository, job) -> None:
        """Generate embedding for a single job.

        Args:
            repo: JobRepository instance (provided by UoW)
            job: JobPost ORM instance (loaded within this UoW session)
        """
        parts = []

        if job.requirements:
            parts.extend([r.text for r in job.requirements[:20]])

        if job.benefits:
            parts.extend([b.text for b in job.benefits[:10]])

        if parts:
            text = " | ".join(parts)
        else:
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
            except IOError as e:
                logger.error(f"Failed to read resume file: {e}")
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
        except (ValueError, IOError) as e:
            logger.error(f"Failed to parse resume file: {e}")
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
        except IOError as exc:
            logger.error(f"Failed to read resume file: {exc}")
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
            logger.error(f"Failed to parse resume file: {exc}")
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
                logger.error(f"Failed to resume embedding for resume: {exc}")
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
            logger.error(f"Failed to process resume: {exc}")
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
        except Exception as e:
            logger.error(f"Failed to process resume: {e}")
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
        except Exception as e:
            logger.error(f"Failed to extract resume: {e}")
            raise

        return False, fingerprint, None

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
                logger.error(f"Failed to parse resume file: {e}")
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
            logger.error(f"Failed to embed resume: {e}")
            raise

    def extract_resume_stage(
        self,
        repo: JobRepository,
        resume_file: str,
        known_fingerprint: Optional[str] = None,
        *,
        owner_id: Optional[Any] = None,
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """Named stage wrapper for split-service extraction."""
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
        """Named stage wrapper for split-service embedding."""
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
