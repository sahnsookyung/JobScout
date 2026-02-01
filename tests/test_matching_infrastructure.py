#!/usr/bin/env python3
"""
Comprehensive tests for matching infrastructure.

This file is organized into two test classes:
1. TestMatchingUnit - Tests that don't require database (pure logic tests)
2. TestMatchingDatabase - Tests that require PostgreSQL database

Run with:
  ./run_tests.sh --unit-only           # Run only unit tests
  ./run_tests.sh --with-db             # Run all tests with DB
  ./run_tests.sh --with-db --db-only   # Run only DB tests

Or manually:
  python -m pytest tests/test_matching_infrastructure.py::TestMatchingUnit -v
  TEST_DATABASE_URL=postgresql://... python -m pytest tests/test_matching_infrastructure.py::TestMatchingDatabase -v
"""

import unittest
import json
import os
import tempfile
from datetime import datetime
from typing import List, Dict, Any
from unittest.mock import MagicMock, patch

import pytest

# Core services (no DB needed)
from core.config_loader import load_config, MatchingConfig, MatcherConfig, ScorerConfig
from core.matcher_service import MatcherService, ResumeEvidenceUnit, MockMatcherService
from core.scorer_service import ScoringService
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


class TestMatchingUnit(unittest.TestCase):
    """
    UNIT TESTS - No database required.
    
    These tests verify core logic, algorithms, and service functionality
    using mocks. They run quickly and don't need Docker/PostgreSQL.
    """
    
    @classmethod
    def setUpClass(cls):
        """Set up test data."""
        cls.resume_data = {
            "name": "Test User",
            "title": "Software Engineer",
            "sections": [
                {
                    "title": "Skills",
                    "items": [
                        {"description": "Java, Python, AWS, Kubernetes", "highlights": []}
                    ]
                },
                {
                    "title": "Experience",
                    "items": [
                        {
                            "company": "TechCorp",
                            "role": "Senior Engineer",
                            "period": "2020-2024",
                            "description": "Built microservices with Java and AWS",
                            "highlights": [
                                "Led team of 5 engineers",
                                "Implemented CI/CD pipelines"
                            ]
                        }
                    ]
                }
            ]
        }
    
    def setUp(self):
        """Set up services with mocks."""
        self.mock_repo = MagicMock()
        self.mock_ai = MagicMock()
        self.mock_ai.generate_embedding = MagicMock(return_value=[0.1] * 1024)
        
        self.matcher_config = MatcherConfig(similarity_threshold=0.3)
        self.scorer_config = ScorerConfig(
            weight_required=0.7,
            weight_preferred=0.3,
            wants_remote=True,
            min_salary=5000000
        )
        
        self.matcher = MockMatcherService(self.mock_repo, self.mock_ai, self.matcher_config)
        self.scorer = ScoringService(self.mock_repo, self.scorer_config)
    
    # ============ UNIT TEST 1: Config Loading ============
    
    def test_01_config_loading(self):
        """Test loading matching config from YAML."""
        print("\n⚙️  UNIT Test 1: Config Loading")
        
        config_content = """
database:
  url: "postgresql://test:test@localhost:5432/test"

matching:
  enabled: true
  mode: "with_preferences"
  resume_file: "test_resume.json"
  preferences_file: "test_prefs.json"
  
  matcher:
    enabled: true
    similarity_threshold: 0.6
    batch_size: 50
    
  scorer:
    enabled: true
    weight_required: 0.8
    weight_preferred: 0.2
    wants_remote: true
    min_salary: 50000

schedule:
  interval_seconds: 3600

scrapers: []
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_content)
            config_path = f.name
        
        try:
            config = load_config(config_path)
            
            self.assertTrue(config.matching.enabled)
            self.assertEqual(config.matching.mode, "with_preferences")
            self.assertEqual(config.matching.matcher.similarity_threshold, 0.6)
            self.assertEqual(config.matching.scorer.weight_required, 0.8)
            
            print(f"  ✓ Config loaded successfully")
            print(f"  ✓ Matching enabled: {config.matching.enabled}")
            print(f"  ✓ Mode: {config.matching.mode}")
            
        finally:
            os.unlink(config_path)
    
    # ============ UNIT TEST 2: Fingerprint Generation ============
    
    def test_02_resume_fingerprint_generation(self):
        """Test resume fingerprint generation."""
        print("\n🔐 UNIT Test 2: Resume Fingerprint")
        
        fp1 = generate_resume_fingerprint(self.resume_data)
        fp2 = generate_resume_fingerprint(self.resume_data)
        
        # Same data = same fingerprint
        self.assertEqual(fp1, fp2)
        
        # Different data = different fingerprint
        modified_data = self.resume_data.copy()
        modified_data["name"] = "Different Name"
        fp3 = generate_resume_fingerprint(modified_data)
        
        self.assertNotEqual(fp1, fp3)
        
        print(f"  ✓ Fingerprint: {fp1[:16]}...")
        print(f"  ✓ Same data = same fingerprint")
        print(f"  ✓ Different data = different fingerprint")
    
    # ============ UNIT TEST 3: Evidence Extraction ============
    
    def test_03_matcher_evidence_extraction(self):
        """Test MatcherService evidence extraction."""
        print("\n🔍 UNIT Test 3: Evidence Extraction")
        
        evidence_units = self.matcher.extract_resume_evidence(self.resume_data)
        
        self.assertGreater(len(evidence_units), 0)
        
        # Check sections
        sections = set(e.source_section for e in evidence_units)
        self.assertIn("Skills", sections)
        self.assertIn("Experience", sections)
        
        print(f"  ✓ Extracted {len(evidence_units)} evidence units")
        print(f"  ✓ Sections: {', '.join(sections)}")
    
    # ============ UNIT TEST 4: Coverage Calculation ============
    
    def test_04_scorer_coverage_calculation(self):
        """Test ScorerService coverage calculation."""
        print("\n📊 UNIT Test 4: Coverage Calculation")
        
        from core.matcher_service import RequirementMatchResult
        
        # Create mock requirement matches
        req1 = MagicMock()
        req1.requirement.req_type = "required"
        req1.is_covered = True
        
        req2 = MagicMock()
        req2.requirement.req_type = "required"
        req2.is_covered = False
        
        req3 = MagicMock()
        req3.requirement.req_type = "preferred"
        req3.is_covered = True
        
        matched = [req1, req3]
        missing = [req2]
        
        required_cov, preferred_cov = self.scorer.calculate_coverage(matched, missing)
        
        self.assertEqual(required_cov, 0.5)  # 1 of 2 required
        self.assertEqual(preferred_cov, 1.0)  # 1 of 1 preferred
        
        print(f"  ✓ Required coverage: {required_cov*100:.0f}%")
        print(f"  ✓ Preferred coverage: {preferred_cov*100:.0f}%")
    
    # ============ UNIT TEST 5: Base Score Calculation ============
    
    def test_05_scorer_base_score(self):
        """Test base score calculation."""
        print("\n📊 UNIT Test 5: Base Score Calculation")
        
        # Full coverage
        score = self.scorer.calculate_base_score(1.0, 1.0)
        self.assertEqual(score, 100.0)
        
        # Partial coverage
        score = self.scorer.calculate_base_score(0.5, 1.0)
        expected = 100 * (0.7 * 0.5 + 0.3 * 1.0)
        self.assertAlmostEqual(score, expected, places=2)
        
        print(f"  ✓ Full coverage: 100.0")
        print(f"  ✓ Partial coverage: {score:.1f}")
    
    # ============ UNIT TEST 6: Penalty Calculation ============
    
    def test_06_scorer_penalty_calculation(self):
        """Test penalty calculation."""
        print("\n📊 UNIT Test 6: Penalty Calculation")
        
        # Create non-remote job
        job = MagicMock()
        job.is_remote = False
        job.location_text = "Tokyo"
        job.salary_max = None
        job.job_level = None
        
        penalties, details = self.scorer.calculate_penalties(
            job, 0.5, [], []
        )
        
        # Should have location penalty (config wants remote)
        self.assertGreater(penalties, 0)
        location_penalty = next((d for d in details if d['type'] == 'location_mismatch'), None)
        self.assertIsNotNone(location_penalty)
        
        print(f"  ✓ Penalties: {penalties:.1f}")
        print(f"  ✓ Has location penalty: {location_penalty is not None}")
    
    # ============ UNIT TEST 7: Complete Scoring ============
    
    def test_07_scorer_complete_scoring(self):
        """Test complete scoring pipeline."""
        print("\n📊 UNIT Test 7: Complete Scoring")
        
        from core.matcher_service import JobMatchPreliminary, RequirementMatchResult
        
        # Create preliminary match
        job = MagicMock()
        job.id = "job-123"
        job.title = "Test Job"
        job.company = "TestCorp"
        job.is_remote = True
        job.salary_max = None
        job.job_level = None
        
        req = MagicMock()
        req.id = "req-1"
        req.req_type = "required"
        req.text = "Python"
        
        req_match = RequirementMatchResult(
            requirement=req,
            evidence=None,
            similarity=0.8,
            is_covered=True
        )
        
        preliminary = JobMatchPreliminary(
            job=job,
            job_similarity=0.75,
            preferences_alignment=None,
            requirement_matches=[req_match],
            missing_requirements=[],
            resume_fingerprint="test_fp"
        )
        
        # Score
        scored = self.scorer.score_preliminary_match(preliminary)
        
        self.assertIsNotNone(scored)
        self.assertGreater(scored.overall_score, 0)
        self.assertEqual(scored.required_coverage, 1.0)
        
        print(f"  ✓ Overall score: {scored.overall_score:.1f}")
        print(f"  ✓ Base score: {scored.base_score:.1f}")
        print(f"  ✓ Required coverage: {scored.required_coverage*100:.0f}%")


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
        
        from core.matcher_service import MockMatcherService
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
        match_record = scorer.save_match_to_db(scored)
        
        self.assertIsNotNone(match_record.id)
        self.assertEqual(match_record.resume_fingerprint, resume_fp)
        
        print(f"  ✓ Complete pipeline executed")
        print(f"  ✓ Match ID: {match_record.id}")
        print(f"  ✓ Overall score: {match_record.overall_score}")
        print(f"  ✓ Saved {len(match_record.requirement_matches)} requirement match(es)")


if __name__ == '__main__':
    # Check if we should run DB tests
    run_db_tests = os.environ.get('TEST_DATABASE_URL') is not None
    
    if not run_db_tests:
        print("\n" + "=" * 60)
        print("Running UNIT TESTS only (no database)")
        print("=" * 60)
        print("\nTo run database tests:")
        print("  1. Start test DB: docker-compose -f docker-compose.test.yml up -d")
        print("  2. Run: TEST_DATABASE_URL=postgresql://testuser:testpass@localhost:5433/jobscout_test python -m pytest tests/test_matching_infrastructure.py::TestMatchingDatabase -v")
        print("  3. Stop: docker-compose -f docker-compose.test.yml down")
        print("\nOr use: ./run_tests.sh --with-db")
        print("=" * 60 + "\n")
        
        # Run only unit tests
        suite = unittest.TestLoader().loadTestsFromTestCase(TestMatchingUnit)
        unittest.TextTestRunner(verbosity=2).run(suite)
    else:
        print("\n" + "=" * 60)
        print("Running ALL TESTS (unit + database)")
        print("=" * 60 + "\n")
        # Run all tests
        unittest.main(verbosity=2)
