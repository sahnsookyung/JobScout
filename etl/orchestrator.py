from typing import Dict, Any, List, Tuple, Optional
import time
import logging
import json
import os
from database.repository import JobRepository
from core.llm.interfaces import LLMProvider
from core.utils import JobFingerprinter
from etl.schemas import EXTRACTION_SCHEMA
from core.scorer.want_score import FACET_KEYS
from database.models import generate_resume_fingerprint
from etl.resume import ResumeProfiler

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

        data = self.ai.extract_structured_data(job.description, EXTRACTION_SCHEMA)

        repo.update_job_metadata(job, data)
        repo.update_content_metadata(job.id, data)
        repo.save_requirements(job, data.get('requirements', []))
        repo.save_benefits(job, data.get('benefits', []))
        repo.mark_as_extracted(job)

    def extract_facets_one(self, repo: JobRepository, job) -> None:
        """Extract job facets for a single job for Want score matching.

        Extracts per-facet text from job descriptions and generates embeddings
        for each of the 7 facets used in Want score calculation.

        Args:
            repo: JobRepository instance (provided by UoW)
            job: JobPost ORM instance (loaded within this UoW session)
        """
        logger.info(f"Extracting facets for job {job.id}: {job.title}")

        facets = self.ai.extract_job_facets(job.description)

        facet_embeddings = {}
        for facet_key in FACET_KEYS:
            facet_text = facets.get(facet_key, "")
            if facet_text:
                embedding = self.ai.generate_embedding(facet_text)
                facet_embeddings[facet_key] = embedding
                # Pass content_hash from job - it should be set during ingest
                content_hash = getattr(job, 'content_hash', None) or ''
                repo.save_job_facet_embedding(
                    job.id, facet_key, facet_text, embedding, content_hash
                )
            else:
                logger.debug(f"Empty facet '{facet_key}' for job {job.id}")

        if facet_embeddings:
            logger.info(f"Saved {len(facet_embeddings)} facet embeddings for job {job.id}")
        else:
            logger.warning(f"No facet embeddings saved for job {job.id}")

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
        resume_file: str
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

        try:
            with open(resume_file, 'r') as f:
                resume_data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Failed to load resume file: {e}")
            return False, "", None

        # Generate fingerprint
        fingerprint = generate_resume_fingerprint(resume_data)
        logger.info(f"Resume fingerprint: {fingerprint[:16]}...")

        # Check if resume already exists (unchanged)
        existing = repo.resume.get_structured_resume_by_fingerprint(fingerprint)
        if existing:
            logger.info(f"Resume unchanged (fingerprint: {fingerprint[:16]}...), skipping ETL")
            return False, fingerprint, resume_data

        logger.info(f"Resume changed (fingerprint: {fingerprint[:16]}...), processing...")

        # Process the resume
        try:
            self.extract_resume_one(repo, resume_data, fingerprint)
            logger.info(f"Resume ETL completed for fingerprint: {fingerprint[:16]}...")
            return True, fingerprint, resume_data
        except Exception as e:
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

        # Create profiler (no store - we handle persistence manually)
        profiler = ResumeProfiler(ai_service=self.ai)

        # Extract structured profile
        structured_profile = profiler.extract_structured_resume(resume_data)

        # Extract evidence units
        evidence_units = profiler.extract_resume_evidence(resume_data)

        # Also extract years from evidence units
        profiler.years_extractor.extract_from_evidence(evidence_units)

        if structured_profile:
            years_msg = f"Total experience: {structured_profile.calculated_total_years} years"
            if structured_profile.claimed_total_years:
                years_msg += f" (claimed: {structured_profile.claimed_total_years})"
            logger.info(years_msg)

            # Validate experience claim
            is_valid, validation_msg = structured_profile.validate_experience_claim()
            if not is_valid:
                logger.warning(f"Experience validation failed: {validation_msg}")

            # Save structured resume to database
            repo.save_structured_resume(
                resume_fingerprint=fingerprint,
                extracted_data=structured_profile.raw_data,
                calculated_total_years=structured_profile.calculated_total_years,
                claimed_total_years=structured_profile.claimed_total_years,
                experience_validated=is_valid,
                validation_message=validation_msg,
                extraction_confidence=structured_profile.raw_data.get('extraction', {}).get('confidence'),
                extraction_warnings=structured_profile.raw_data.get('extraction', {}).get('warnings', [])
            )
            logger.info(f"Saved structured resume to database")

        # Generate and save embeddings for evidence units
        if evidence_units:
            logger.info(f"Generating embeddings for {len(evidence_units)} evidence units...")
            profiler.embed_evidence_units(evidence_units)

            # Save evidence unit embeddings
            sections = []
            for i, unit in enumerate(evidence_units):
                if unit.embedding:
                    sections.append({
                        'section_type': unit.source_section,
                        'section_index': i,
                        'embedding': unit.embedding,
                        'text': unit.text
                    })

            if sections:
                repo.save_resume_section_embeddings(fingerprint, sections)
                logger.info(f"Saved {len(sections)} evidence unit embeddings")

        units_with_years = [u for u in evidence_units if u.years_value is not None]
        total_years_units = [u for u in units_with_years if u.is_total_years_claim]
        logger.info(f"Extracted years from {len(units_with_years)} evidence units ({len(total_years_units)} total claims)")

    def unload_models(self):
        """Helper to unload models if the provider supports it."""
        if hasattr(self.ai, 'unload_model'):
            self.ai.unload_model(self.ai.extraction_model)
            self.ai.unload_model(self.ai.embedding_model)
