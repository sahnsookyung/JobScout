#!/usr/bin/env python3
"""
Comprehensive tests for matching infrastructure.

This file contains integration tests that require PostgreSQL database.
For unit tests that don't require database, see tests/unit/core/.

Run with:
  ./run_tests.sh --with-db             # Run all tests with DB
  ./run_tests.sh --with-db --db-only   # Run only DB tests

Or manually:
  TEST_DATABASE_URL=postgresql://... python -m pytest tests/integration/test_matching_infrastructure.py -v
"""

import unittest
import json
import os
from datetime import datetime
from typing import List, Dict, Any
from unittest.mock import MagicMock, patch

import pytest

# Core services (no DB needed)
from core.config_loader import load_config, MatchingConfig, MatcherConfig, ScorerConfig
from core.matcher import MatcherService, ResumeEvidenceUnit
from tests.mocks.matcher_mocks import MockMatcherService
from core.scorer import ScoringService
from core.scorer import persistence
from database.models import generate_resume_fingerprint

# Database imports (only needed for DB tests)
try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker, Session
    from database.models import (
        Base, JobPost, JobRequirementUnit, JobRequirementUnitEmbedding,
        JobMatch, JobMatchRequirement
    )
    from database.repository import JobRepository
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False


@pytest.mark.db
class TestMatchingDatabase(unittest.TestCase):
    """
    DATABASE TESTS - Require PostgreSQL with pgvector.
    
    These tests verify database operations, models, and repository methods.
    They require a running PostgreSQL instance with pgvector extension.
    
    The test database is automatically managed by the test_database fixture
    in conftest.py using testcontainers. It will start a container before
    tests and stop it after all tests complete.
    
    To run these tests:
      pytest tests/test_matching_infrastructure.py::TestMatchingDatabase -v
    
    Or with external database:
      TEST_DATABASE_URL=postgresql://... pytest tests/test_matching_infrastructure.py::TestMatchingDatabase -v
    """
    
    @pytest.fixture(scope="class")
    def db_engine(self, test_database):
        """Create database engine for the test class."""
        engine = create_engine(test_database)
        Base.metadata.create_all(engine)
        yield engine
        Base.metadata.drop_all(engine)
    
    @pytest.fixture(scope="class")
    def db_sessionmaker(self, db_engine):
        """Create sessionmaker for the test class."""
        return sessionmaker(bind=db_engine)
    
    @pytest.fixture(scope="class")
    def _resume_data(self):
        """Load test resume data."""
        resume_path = os.path.join(os.path.dirname(__file__), '..', 'resume.json')
        if os.path.exists(resume_path):
            with open(resume_path, 'r') as f:
                return json.load(f)
        else:
            return {
                "name": "Test User",
                "sections": [
                    {"title": "Skills", "items": [{"description": "Python, Java", "highlights": []}]}
                ]
            }
    
    @pytest.fixture(autouse=True)
    def setup(self, db_sessionmaker, _resume_data):
        """Set up test session for each test method."""
        self.SessionLocal = db_sessionmaker
        self.resume_data = _resume_data
        self.session = self.SessionLocal()
        self.repo = JobRepository(self.session)
        
        # Clean up any existing test data
        self.session.query(JobMatchRequirement).delete()
        self.session.query(JobMatch).delete()
        self.session.query(JobRequirementUnitEmbedding).delete()
        self.session.query(JobRequirementUnit).delete()
        self.session.query(JobPost).delete()
        self.session.commit()
        yield
        # Cleanup after each test
        self.session.rollback()
        self.session.close()
    
    def _create_test_job(self, title: str, company: str, is_remote: bool = True) -> JobPost:
        """Helper to create a test job."""
        job = JobPost(
            title=title,
            company=company,
            location_text="Remote" if is_remote else "Tokyo, Japan",
            is_remote=is_remote,
            canonical_fingerprint=f"hash_{title}_{company}",
            description=f"Test job for {title}",
            is_embedded=True,
            summary_embedding=[0.1] * 1024
        )
        self.session.add(job)
        self.session.flush()
        return job
    
    def _create_test_requirement(
        self, 
        job: JobPost, 
        text: str, 
        req_type: str = "required"
    ) -> JobRequirementUnit:
        """Helper to create a test requirement."""
        req = JobRequirementUnit(
            job_post_id=job.id,
            req_type=req_type,
            text=text,
            tags={"skill": text.split()[0].lower()}
        )
        self.session.add(req)
        self.session.flush()
        
        # Add embedding
        emb = JobRequirementUnitEmbedding(
            job_requirement_unit_id=req.id,
            embedding=[0.2] * 1024
        )
        self.session.add(emb)
        self.session.flush()
        
        return req
    
    # ============ DB TEST 1: JobMatch Model ============
    
    def test_db_01_job_match_model(self):
        """Test JobMatch database model creation."""
        print("\n💾 DB Test 1: JobMatch Model")
        
        job = self._create_test_job("Python Developer", "TechCorp")
        
        resume_fp = generate_resume_fingerprint(self.resume_data)
        match = JobMatch(
            job_post_id=job.id,
            resume_fingerprint=resume_fp,
            overall_score=85.5,
            base_score=95.0,
            penalties=9.5,
            required_coverage=0.9,
            preferred_coverage=0.8,
            total_requirements=5,
            matched_requirements_count=4,
            match_type="requirements_only"
        )
        
        self.session.add(match)
        self.session.commit()
        
        self.assertIsNotNone(match.id)
        self.assertEqual(match.overall_score, 85.5)
        
        print(f"  ✓ Created JobMatch with ID: {match.id}")
        print(f"  ✓ Score: {match.overall_score}")
    
    # ============ DB TEST 2: JobMatchRequirement Model ============
    
    def test_db_02_job_match_requirement_model(self):
        """Test JobMatchRequirement database model."""
        print("\n💾 DB Test 2: JobMatchRequirement Model")
        
        job = self._create_test_job("Backend Engineer", "DataCorp")
        req = self._create_test_requirement(job, "5+ years Python experience")
        
        match = JobMatch(
            job_post_id=job.id,
            resume_fingerprint="test_fp_123",
            overall_score=90.0,
            match_type="requirements_only"
        )
        self.session.add(match)
        self.session.flush()
        
        req_match = JobMatchRequirement(
            job_match_id=match.id,
            job_requirement_unit_id=req.id,
            evidence_text="Built Python microservices",
            evidence_section="Experience",
            similarity_score=0.85,
            is_covered=True,
            req_type="required"
        )
        
        self.session.add(req_match)
        self.session.commit()
        
        self.assertIsNotNone(req_match.id)
        self.assertAlmostEqual(float(req_match.similarity_score), 0.85, places=2)
        
        print(f"  ✓ Created JobMatchRequirement with ID: {req_match.id}")
        print(f"  ✓ Similarity: {req_match.similarity_score}")
    
    # ============ DB TEST 3: Repository Get Embedded Jobs ============
    
    def test_db_03_repository_get_embedded_jobs(self):
        """Test repository method for getting embedded jobs."""
        print("\n💾 DB Test 3: Repository - Get Embedded Jobs")
        
        job_embedded = self._create_test_job("Embedded Job", "Corp1")
        
        job_not_embedded = JobPost(
            title="Not Embedded",
            company="Corp2",
            canonical_fingerprint="hash_not_emb",
            is_embedded=False
        )
        self.session.add(job_not_embedded)
        self.session.commit()
        
        embedded_jobs = self.repo.get_embedded_jobs_for_matching(limit=100)
        
        self.assertIn(job_embedded, embedded_jobs)
        self.assertNotIn(job_not_embedded, embedded_jobs)
        
        print(f"  ✓ Found {len(embedded_jobs)} embedded jobs")
    
    # ============ DB TEST 4: Repository Match Invalidation ============
    
    def test_db_04_repository_match_invalidation(self):
        """Test repository match invalidation."""
        print("\n💾 DB Test 4: Repository - Match Invalidation")
        
        job = self._create_test_job("Test Job", "TestCorp")
        match = JobMatch(
            job_post_id=job.id,
            resume_fingerprint="fp_123",
            overall_score=80.0,
            status="active"
        )
        self.session.add(match)
        self.session.commit()
        
        count = self.repo.invalidate_matches_for_job(job.id, "Job updated")
        
        self.assertEqual(count, 1)
        self.assertEqual(match.status, "stale")
        
        print(f"  ✓ Invalidated {count} match(es)")
        print(f"  ✓ Match status: {match.status}")
    
    # ============ DB TEST 5: Repository Get Matches for Resume ============
    
    def test_db_05_repository_get_matches_for_resume(self):
        """Test retrieving matches for a resume."""
        print("\n💾 DB Test 5: Repository - Get Matches for Resume")
        
        resume_fp = "test_resume_fp"
        
        job1 = self._create_test_job("Job 1", "Corp1")
        job2 = self._create_test_job("Job 2", "Corp2")
        
        match1 = JobMatch(
            job_post_id=job1.id,
            resume_fingerprint=resume_fp,
            overall_score=90.0,
            status="active"
        )
        match2 = JobMatch(
            job_post_id=job2.id,
            resume_fingerprint=resume_fp,
            overall_score=75.0,
            status="active"
        )
        self.session.add_all([match1, match2])
        self.session.commit()
        
        matches = self.repo.get_matches_for_resume(resume_fp, min_score=80.0)
        
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].overall_score, 90.0)
        
        print(f"  ✓ Found {len(matches)} match(es) with score >= 80")
    
    # ============ DB TEST 6: End-to-End ============
    
    def test_db_06_end_to_end_matching_pipeline(self):
        """Test complete end-to-end matching pipeline."""
        print("\n💾 DB Test 6: End-to-End Matching Pipeline")
        
        from tests.mocks.matcher_mocks import MockMatcherService
        from core.config_loader import MatcherConfig, ScorerConfig
        
        # Create job with requirements
        job = self._create_test_job("Senior Python Dev", "TechCorp", is_remote=True)
        req1 = self._create_test_requirement(job, "Python expertise", "required")
        req2 = self._create_test_requirement(job, "AWS knowledge", "preferred")
        self.session.commit()
        
        # Stage 1: Match
        matcher_config = MatcherConfig(similarity_threshold=0.3)
        matcher = MockMatcherService(self.repo, MagicMock(), matcher_config)
        
        evidence_units = matcher.extract_resume_evidence(self.resume_data)
        matcher.embed_evidence_units(evidence_units)
        
        from database.models import generate_resume_fingerprint
        resume_fp = generate_resume_fingerprint(self.resume_data)
        preliminary = matcher.match_resume_to_job(evidence_units, job, resume_fp)
        
        # Stage 2: Score
        scorer_config = ScorerConfig(wants_remote=True)
        scorer = ScoringService(self.repo, scorer_config)
        
        scored = scorer.score_preliminary_match(preliminary)
        
        # Stage 3: Save
        match_record = persistence.save_match_to_db(scored, scorer.repo)
        
        self.assertIsNotNone(match_record.id)
        self.assertEqual(match_record.resume_fingerprint, resume_fp)
        
        print(f"  ✓ Complete pipeline executed")
        print(f"  ✓ Match ID: {match_record.id}")
        print(f"  ✓ Overall score: {match_record.overall_score}")
        print(f"  ✓ Saved {len(match_record.requirement_matches)} requirement match(es)")


if __name__ == '__main__':
    print("\n" + "=" * 60)
    print("DATABASE INTEGRATION TESTS")
    print("=" * 60)
    print("\nThese tests require a PostgreSQL database with pgvector.")
    print("\nTo run database tests:")
    print("  1. Start test DB: docker-compose -f docker-compose.test.yml up -d")
    print("  2. Run: TEST_DATABASE_URL=postgresql://testuser:testpass@localhost:5433/jobscout_test python -m pytest tests/integration/test_matching_infrastructure.py -v")
    print("  3. Stop: docker-compose -f docker-compose.test.yml down")
    print("\nOr use: ./run_tests.sh --with-db")
    print("=" * 60 + "\n")

    # Tests must be run via pytest due to fixture dependencies
    import sys
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
