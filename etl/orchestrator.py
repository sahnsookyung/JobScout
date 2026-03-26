from typing import Dict, Any, List, Tuple, Optional
import time
import logging
import json
import os
from datetime import datetime, timezone
from database.repository import JobRepository
from core.llm.interfaces import LLMProvider
from core.utils import JobFingerprinter
from pydantic import ValidationError
from core.llm.schema_models import JobExtraction
from core.llm.schema_models import ResumeSchema
from core.scorer.want_score import FACET_KEYS
from database.models import (
    generate_file_fingerprint,
    RESUME_PROCESSING_EXTRACTING,
    RESUME_PROCESSING_EXTRACTED,
    RESUME_PROCESSING_EMBEDDING,
    RESUME_PROCESSING_READY,
    RESUME_PROCESSING_FAILED,
)
from etl.resume import ResumeProfiler, ResumeParser
from etl.resume.embedding_store import JobRepositoryAdapter

logger = logging.getLogger(__name__)


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

    def embed_facets_one(self, repo: JobRepository, job) -> None:
        """Generate embeddings for extracted facets of a single job.

        Args:
            repo: JobRepository instance (provided by UoW)
            job: JobPost ORM instance (loaded within this UoW session)
        """
        logger.info(f"Embedding facets for job {job.id}: {job.title}")

        try:
            facets = repo.get_facets_for_job(job.id)
            if not facets:
                logger.debug(f"No facets found for job {job.id}")
                return

            content_hash = job.content_hash or ''
            saved_count = 0

            for facet in facets:
                if facet.embedding is None:
                    embedding = self.ai.generate_embedding(facet.text)
                    repo.update_facet_embedding(facet.id, embedding, content_hash)
                    saved_count += 1

            logger.info(f"Embedded {saved_count} facets for job {job.id}")

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

        vector = self.ai.generate_embedding(text)

        repo.save_job_embedding(job, vector)

    def embed_requirement_one(self, repo: JobRepository, req) -> None:
        """Generate embedding for a single requirement.

        Args:
            repo: JobRepository instance (provided by UoW)
            req: JobRequirementUnit ORM instance (loaded within this UoW session)
        """
        vector = self.ai.generate_embedding(req.text)
        repo.save_requirement_embedding(req.id, vector)

    def process_resume(
        self,
        repo: JobRepository,
        resume_file: str,
        force_re_extraction: bool = False,
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """Process resume ETL with fingerprint-based change detection.

        This method implements the resume ETL step that should be called
        within a job_uow() context manager. It checks if the resume has
        changed using fingerprinting and only processes if needed.

        Args:
            repo: JobRepository instance (provided by UoW)
            resume_file: Path to resume JSON file

        Returns:
            Tuple of (resume_changed: bool, fingerprint: str, resume_data: dict or None)
            - resume_changed: True if resume was processed (changed), False if unchanged
            - fingerprint: The resume fingerprint
            - resume_data: The loaded resume data (or None if file not found)
        """
        # Load resume file
        if not os.path.exists(resume_file):
            logger.error(f"Resume file not found: {resume_file}")
            return False, "", None

        # Generate fingerprint from raw file bytes
        try:
            with open(resume_file, 'rb') as f:
                file_bytes = f.read()
            fingerprint = generate_file_fingerprint(file_bytes)
        except IOError as e:
            logger.error(f"Failed to read resume file: {e}")
            return False, "", None

        logger.info(f"Resume fingerprint: {fingerprint[:16]}...")

        if repo.is_resume_ready(fingerprint) and not force_re_extraction:
            logger.info(f"Resume ready (fingerprint: {fingerprint[:16]}...), skipping ETL")
            return False, fingerprint, None

        state = repo.get_resume_processing_state(fingerprint)
        if state and state.processing_status in {
            RESUME_PROCESSING_EXTRACTING,
            RESUME_PROCESSING_EMBEDDING,
        }:
            logger.info(
                "Resume already processing (fingerprint: %s..., status=%s)",
                fingerprint[:16],
                state.processing_status,
            )
            return False, fingerprint, None

        # Parse for processing
        try:
            parser = ResumeParser()
            parsed = parser.parse(resume_file)
            resume_data = parsed.data if parsed.data is not None else {"raw_text": parsed.text}
        except (ValueError, IOError) as e:
            repo.set_resume_processing_state(
                fingerprint,
                RESUME_PROCESSING_FAILED,
                error=str(e),
            )
            logger.error(f"Failed to parse resume file: {e}")
            return False, "", None

        if state and state.processing_status == RESUME_PROCESSING_EXTRACTED:
            logger.info(
                "Resume extracted but not ready (fingerprint: %s...), resuming embedding",
                fingerprint[:16],
            )
            try:
                self.embed_resume_one(repo, fingerprint)
                logger.info(f"Resume embedding completed for fingerprint: {fingerprint[:16]}...")
                return True, fingerprint, resume_data
            except Exception as e:
                repo.set_resume_processing_state(
                    fingerprint,
                    RESUME_PROCESSING_FAILED,
                    error=str(e),
                )
                logger.error(f"Failed to resume embedding for resume: {e}")
                raise

        if force_re_extraction:
            logger.info(
                "Force re-extraction enabled for fingerprint: %s...",
                fingerprint[:16],
            )
        else:
            logger.info(f"Resume changed (fingerprint: {fingerprint[:16]}...), processing...")

        # Process the resume
        try:
            repo.set_resume_processing_state(
                fingerprint,
                RESUME_PROCESSING_EXTRACTING,
                error=None,
            )
            self.extract_resume_one(repo, resume_data, fingerprint)
            logger.info(f"Resume ETL completed for fingerprint: {fingerprint[:16]}...")
            return True, fingerprint, resume_data
        except Exception as e:
            repo.set_resume_processing_state(
                fingerprint,
                RESUME_PROCESSING_FAILED,
                error=str(e),
            )
            logger.error(f"Failed to process resume: {e}")
            raise

    def extract_resume_one(
        self,
        repo: JobRepository,
        resume_data: Dict[str, Any],
        fingerprint: str
    ) -> None:
        """Extract structured data from resume and save to DB.

        Args:
            repo: JobRepository instance (provided by UoW)
            resume_data: Raw resume JSON data
            fingerprint: Resume fingerprint
        """
        logger.info(f"Extracting structured resume data...")

        profiler = ResumeProfiler(ai_service=self.ai)

        resume = profiler.extract_structured_resume(resume_data)

        if not resume:
            raise ValueError("Structured resume extraction failed")

        years_msg = f"Total experience: {resume.claimed_total_years or 'unknown'} years"
        logger.info(years_msg)

        repo.save_structured_resume(
            resume_fingerprint=fingerprint,
            extracted_data=resume.model_dump(),
            total_experience_years=resume.claimed_total_years,
            extraction_confidence=resume.extraction.confidence if resume.extraction else None,
            extraction_warnings=resume.extraction.warnings if resume.extraction else []
        )
        logger.info("Saved structured resume to database")

        repo.set_resume_processing_state(
            fingerprint,
            RESUME_PROCESSING_EXTRACTED,
            error=None,
            extraction_completed_at=datetime.now(timezone.utc),
        )

        self.embed_resume_one(repo, fingerprint, resume)

    def embed_resume_one(
        self,
        repo: JobRepository,
        fingerprint: str,
        pre_extracted_resume: Optional[ResumeSchema] = None,
    ) -> None:
        """Generate resume embeddings from a structured resume and mark readiness."""
        repo.set_resume_processing_state(
            fingerprint,
            RESUME_PROCESSING_EMBEDDING,
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
        )

        if not persistence_payload:
            raise ValueError("No resume section embeddings were generated")

        if not evidence_units:
            raise ValueError("No resume evidence embeddings were generated")

        if repo.get_resume_summary_embedding(fingerprint) is None:
            raise ValueError("No summary embedding found after resume embedding")

        repo.set_resume_processing_state(
            fingerprint,
            RESUME_PROCESSING_READY,
            error=None,
            embedding_completed_at=datetime.now(timezone.utc),
        )

    def unload_models(self):
        """Helper to unload models if the provider supports it."""
        if hasattr(self.ai, 'unload_model'):
            self.ai.unload_model(self.ai.extraction_model)
            self.ai.unload_model(self.ai.embedding_model)
