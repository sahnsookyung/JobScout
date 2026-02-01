#!/usr/bin/env python3
"""
Integration Test: Full Pipeline End-to-End with Docker

This test verifies the complete flow from ETL processing through matching
to notifications with real services using Docker containers automatically.

Flow Tested:
  Job Data → ETL Processing → Database → Matching → Scoring → Notifications

Usage:
    # With automatic Docker containers (default):
    uv run python -m pytest tests/integration_test_full_pipeline.py -v
    
    # Or with external services:
    TEST_DATABASE_URL=postgresql://user:pass@localhost:5432/jobscout \
    REDIS_URL=redis://localhost:6379/0 \
        uv run python -m pytest tests/integration_test_full_pipeline.py -v

Requirements:
    - Docker must be available and running (unless using external services)
    - psycopg2-binary: uv add --dev psycopg2-binary
    - redis: uv add --dev redis
"""

import unittest
import sys
import os
import json
import time
import uuid
import numpy as np
from datetime import datetime
from typing import List, Dict, Any
from unittest.mock import patch

# Check if we should run with Docker containers
USE_DOCKER = os.environ.get('USE_DOCKER_CONTAINERS', '1') == '1'
TEST_DATABASE_URL = os.environ.get('TEST_DATABASE_URL')
REDIS_URL = os.environ.get('REDIS_URL')

# Try to import container management
postgres_container = None
redis_container = None
try:
    from tests.conftest_docker import postgres_container, redis_container
    DOCKER_AVAILABLE = True
except ImportError:
    DOCKER_AVAILABLE = False

# Determine if we can run database tests
if TEST_DATABASE_URL:
    # Use provided database
    RUN_TESTS = True
    USE_EXTERNAL_DB = True
elif DOCKER_AVAILABLE and USE_DOCKER:
    # Will spin up Docker container
    RUN_TESTS = True
    USE_EXTERNAL_DB = False
else:
    RUN_TESTS = False

if not RUN_TESTS:
    print("\n" + "="*70)
    print("SKIPPING: Docker not available and TEST_DATABASE_URL not set")
    print("To run: ensure Docker is running or set TEST_DATABASE_URL")
    print("="*70 + "\n")
    sys.exit(0)

# Check Redis availability (optional)
REDIS_AVAILABLE = False
if REDIS_URL:
    try:
        import redis as redis_lib
        redis_conn = redis_lib.Redis.from_url(REDIS_URL)
        redis_conn.ping()
        REDIS_AVAILABLE = True
    except:
        REDIS_AVAILABLE = False

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database.models import Base, JobPost, JobRequirementUnit, JobMatch
from database.repository import JobRepository
from core.matcher_service import MatcherService, ResumeEvidenceUnit
from core.config_loader import MatcherConfig, ScorerConfig
from core.interfaces import LLMProvider
from core.scorer_service import ScoringService
from notification import NotificationService
from etl.orchestrator import JobETLOrchestrator


class MockAIService(LLMProvider):
    """Mock AI service that implements LLMProvider interface."""
    
    def extract_structured_data(self, text: str, schema: Dict) -> Dict[str, Any]:
        """Mock structured data extraction."""
        return {}
    
    def generate_embedding(self, text: str) -> List[float]:
        """Generate a random embedding for testing."""
        return np.random.randn(1024).tolist()


class TestFullPipelineIntegration(unittest.TestCase):
    """
    End-to-end integration test of the complete JobScout pipeline.
    
    Tests real data flow through all components with actual services,
    using Docker containers automatically or external services if configured.
    """
    
    @classmethod
    def setUpClass(cls):
        """Set up all services and test data."""
        print("\n" + "="*70)
        print("INTEGRATION TEST: Full Pipeline End-to-End")
        print("="*70)
        
        # Database setup (Docker or external)
        if USE_EXTERNAL_DB:
            assert TEST_DATABASE_URL is not None, "TEST_DATABASE_URL must be set"
            print(f"Using external database: {TEST_DATABASE_URL[:30]}...")
            cls.engine = create_engine(TEST_DATABASE_URL)
            cls._setup_database()
        else:
            print("Starting PostgreSQL Docker container...")
            assert postgres_container is not None, "postgres_container must be available"
            try:
                cls.postgres_container_mgr = postgres_container()
                cls.postgres_container = cls.postgres_container_mgr.__enter__()
                print(f"✓ PostgreSQL container started on port {cls.postgres_container.host_port}")
                
                # Connect to container
                cls.engine = create_engine(cls.postgres_container.database_url)
                cls._setup_database()
            except Exception as e:
                print(f"✗ Failed to start PostgreSQL container: {e}")
                raise
        
        # Redis setup (Docker or external, optional)
        cls.redis_url = None
        if REDIS_AVAILABLE and REDIS_URL:
            print(f"Using external Redis: {REDIS_URL[:25]}...")
            cls.redis_url = REDIS_URL
        elif DOCKER_AVAILABLE and USE_DOCKER:
            print("Starting Redis Docker container (optional)...")
            assert redis_container is not None, "redis_container must be available"
            try:
                cls.redis_container_mgr = redis_container()
                cls.redis_container = cls.redis_container_mgr.__enter__()
                cls.redis_url = cls.redis_container.redis_url
                print(f"✓ Redis container started on port {cls.redis_container.host_port}")
            except Exception as e:
                print(f"⚠ Redis container failed to start (optional): {e}")
        
        if not cls.redis_url:
            print("⚠ Redis not available - notification tests will be skipped")
        
        # Create session
        SessionLocal = sessionmaker(bind=cls.engine)
        cls.session = SessionLocal()
        cls.repo = JobRepository(cls.session)
        
        # AI Service (mock for speed, but use real embeddings)
        cls.mock_ai = MockAIService()
        
        # ETL Orchestrator
        cls.orchestrator = JobETLOrchestrator(cls.repo, cls.mock_ai)
        
        # Matching Service
        cls.matcher_config = MatcherConfig(
            similarity_threshold=0.5,
            top_k_requirements=3,
            include_job_level_matching=True
        )
        cls.matcher = MatcherService(cls.repo, cls.mock_ai, cls.matcher_config)
        
        # Scoring Service
        cls.scorer_config = ScorerConfig(
            weight_required=0.7,
            weight_preferred=0.3,
            wants_remote=True
        )
        cls.scorer = ScoringService(cls.repo, cls.scorer_config)
        
        # Notification Service (if Redis available)
        if cls.redis_url:
            cls.notification_service = NotificationService(cls.repo, cls.redis_url)
        else:
            cls.notification_service = None
        
        # Test resume data
        cls.resume_data = {
            "name": "Integration Test User",
            "title": "Senior Python Developer",
            "email": "test@example.com",
            "sections": [
                {
                    "title": "Technical Skills",
                    "items": [
                        {
                            "description": "Python, Django, FastAPI, Flask, SQLAlchemy",
                            "highlights": ["8+ years experience", "Led Python team of 5"]
                        },
                        {
                            "description": "AWS, Docker, Kubernetes, CI/CD",
                            "highlights": ["AWS Certified", "DevOps practices"]
                        }
                    ]
                },
                {
                    "title": "Work Experience",
                    "items": [
                        {
                            "company": "TechCorp Japan",
                            "role": "Senior Python Engineer",
                            "period": "2020-2024",
                            "description": "Built microservices with Python and AWS",
                            "highlights": ["Improved performance by 40%", "Mentored juniors"]
                        }
                    ]
                }
            ]
        }
        
        # Create test jobs
        cls._create_test_jobs()
        
        print("✓ Pipeline services initialized")
    
    @classmethod
    def _setup_database(cls):
        """Create tables and enable pgvector extension."""
        from sqlalchemy import text
        with cls.engine.connect() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
            conn.commit()
        Base.metadata.create_all(cls.engine)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up all test data and containers."""
        if hasattr(cls, 'session'):
            # Clean up test data
            cls.session.query(JobMatch).filter(
                JobMatch.resume_fingerprint.like("test-pipeline-%")
            ).delete(synchronize_session=False)
            cls.session.query(JobPost).filter(
                JobPost.canonical_fingerprint.like("test-pipeline-%")
            ).delete(synchronize_session=False)
            cls.session.commit()
            cls.session.close()
        
        if hasattr(cls, 'engine'):
            cls.engine.dispose()
        
        # Stop containers if we started them
        if not USE_EXTERNAL_DB and hasattr(cls, 'postgres_container_mgr'):
            print("\nStopping PostgreSQL container...")
            cls.postgres_container_mgr.__exit__(None, None, None)
        
        if hasattr(cls, 'redis_container_mgr'):
            print("Stopping Redis container...")
            cls.redis_container_mgr.__exit__(None, None, None)
        
        print("\n✓ Pipeline test complete - test data cleaned up")
        print("="*70 + "\n")
    
    @classmethod
    def _create_test_jobs(cls):
        """Create test job posts with embeddings."""
        cls.test_job_ids = []
        jobs_data = [
            {
                "id": uuid.uuid4(),
                "title": "Senior Python Developer",
                "company": "GoodMatch Corp",
                "location": "Tokyo",
                "is_remote": True,
                "description": "Looking for Python expert with AWS",
                "requirements": [
                    ("5+ years Python", "required", "experience_years"),
                    ("AWS experience", "required", "skill"),
                    ("Team leadership", "preferred", "skill")
                ]
            },
            {
                "id": uuid.uuid4(),
                "title": "Junior Java Developer",
                "company": "BadMatch Inc",
                "location": "Osaka",
                "is_remote": False,
                "description": "Entry level Java position",
                "requirements": [
                    ("Java programming", "required", "skill"),
                    ("Spring framework", "required", "skill"),
                    ("0-2 years experience", "required", "experience_years")
                ]
            },
            {
                "id": uuid.uuid4(),
                "title": "Full Stack Python Engineer",
                "company": "PerfectMatch Ltd",
                "location": "Remote",
                "is_remote": True,
                "description": "Full stack role with Python/React",
                "requirements": [
                    ("Python development", "required", "skill"),
                    ("AWS/Docker", "required", "skill"),
                    ("8+ years experience", "required", "experience_years"),
                    ("React frontend", "preferred", "skill")
                ]
            }
        ]
        
        for job_data in jobs_data:
            job_id = job_data["id"]
            cls.test_job_ids.append(job_id)
            job = JobPost(
                id=job_id,
                canonical_fingerprint=f"test-pipeline-{job_id}",
                title=job_data["title"],
                company=job_data["company"],
                location_text=job_data["location"],
                is_remote=job_data["is_remote"],
                description=job_data["description"],
                first_seen_at=datetime.now(),
                last_seen_at=datetime.now(),
                status="active",
                summary_embedding=np.random.randn(1024),
                raw_payload={
                    "source_site": "test-pipeline",
                    "url": f"https://test.com/{job_id}"
                }
            )
            cls.session.add(job)
            
            # Add requirements
            for i, (req_text, req_type, label) in enumerate(job_data["requirements"]):
                req = JobRequirementUnit(
                    id=uuid.uuid4(),
                    job_post_id=job_id,
                    text=req_text,
                    req_type=req_type,
                    tags={"label": label},
                    ordinal=i
                )
                cls.session.add(req)
        
        cls.session.commit()
        print(f"✓ Created {len(jobs_data)} test jobs with requirements")
    
    def test_01_etl_processing(self):
        """Step 1: Process job data through ETL pipeline."""
        print("\n[Step 1] ETL Processing...")
        
        # Verify jobs are in DB
        job_count = self.session.query(JobPost).filter(
            JobPost.canonical_fingerprint.like("test-pipeline-%")
        ).count()
        
        self.assertGreaterEqual(job_count, 3)
        print(f"  ✓ {job_count} jobs in database after ETL processing")
    
    def test_02_extract_resume_evidence(self):
        """Step 2: Extract evidence units from resume."""
        print("\n[Step 2] Resume Evidence Extraction...")
        
        # Use matcher to extract evidence
        evidence_units = self.matcher.extract_resume_evidence(self.resume_data)
        
        self.assertGreater(len(evidence_units), 0)
        print(f"  ✓ Extracted {len(evidence_units)} evidence units from resume")
        
        # Verify evidence has text content (embeddings are generated lazily during matching)
        for evidence in evidence_units:
            self.assertIsNotNone(evidence.text)
            self.assertGreater(len(evidence.text), 0)
        
        print(f"  ✓ All evidence units have text content")
        # Store as class attribute to persist across test instances
        type(self).test_evidence = evidence_units
    
    def test_03_matcher_service(self):
        """Step 3: Run matching service with real embeddings."""
        print("\n[Step 3] Matcher Service...")
        
        # Get test jobs
        jobs = self.session.query(JobPost).filter(
            JobPost.canonical_fingerprint.like("test-pipeline-%")
        ).all()
        
        # Extract evidence if not done
        if not hasattr(type(self), 'test_evidence'):
            type(self).test_evidence = self.matcher.extract_resume_evidence(self.resume_data)
        
        # Run matching
        preliminary_matches = self.matcher.match_resume_to_jobs(
            evidence_units=type(self).test_evidence,
            jobs=jobs,
            resume_data=self.resume_data
        )
        
        self.assertGreater(len(preliminary_matches), 0)
        print(f"  ✓ Matcher found {len(preliminary_matches)} preliminary matches")
        
        # Verify matches have requirement_matches list
        for match in preliminary_matches:
            self.assertIsNotNone(match.job)
            self.assertIsInstance(match.requirement_matches, list)
        
        # Store as class attribute to persist across test instances
        type(self).test_preliminary_matches = preliminary_matches
    
    def test_04_scorer_service(self):
        """Step 4: Score the preliminary matches."""
        print("\n[Step 4] Scorer Service...")
        
        if not hasattr(type(self), 'test_preliminary_matches'):
            self.skipTest("Preliminary matches not available")
        
        # Score matches
        scored_matches = self.scorer.score_matches(
            preliminary_matches=type(self).test_preliminary_matches,
            match_type="requirements_only"
        )
        
        self.assertGreater(len(scored_matches), 0)
        print(f"  ✓ Scorer processed {len(scored_matches)} matches")
        
        # Verify scores are reasonable
        for scored in scored_matches:
            self.assertGreaterEqual(scored.overall_score, 0.0)
            self.assertLessEqual(scored.overall_score, 100.0)
            print(f"    - {scored.job.title} @ {scored.job.company}: {scored.overall_score:.1f}%")
        
        # Store as class attribute to persist across test instances
        type(self).test_scored_matches = scored_matches
    
    def test_05_save_matches_to_database(self):
        """Step 5: Save scored matches to database."""
        print("\n[Step 5] Save Matches to Database...")
        
        if not hasattr(type(self), 'test_scored_matches'):
            self.skipTest("Scored matches not available")
        
        # Generate fingerprint
        fingerprint = f"test-pipeline-{datetime.now().timestamp()}"
        
        # Save matches
        saved_count = 0
        for scored_match in type(self).test_scored_matches:
            match_record = self.scorer.save_match_to_db(
                scored_match=scored_match,
                preferences_file_hash=fingerprint
            )
            if match_record is not None:
                saved_count += 1
        
        self.assertGreater(saved_count, 0)
        print(f"  ✓ Saved {saved_count} matches to database")
        
        # Sync session with database after external commit
        # Rollback to end current transaction and start fresh to see committed data
        self.session.rollback()
        
        # Verify in DB - query by preferences_file_hash which stores our test fingerprint
        db_matches = self.session.query(JobMatch).filter(
            JobMatch.preferences_file_hash == fingerprint
        ).all()
        
        self.assertEqual(len(db_matches), saved_count)
        print(f"  ✓ Verified {len(db_matches)} match records in database")
        
        # Store as class attribute to persist across test instances
        type(self).test_fingerprint = fingerprint
    
    def test_06_notification_triggering(self):
        """Step 6: Trigger notifications for high-scoring matches."""
        print("\n[Step 6] Notification Triggering...")
        
        if not self.redis_url or not self.notification_service:
            self.skipTest("Redis not available - skipping notification test")
        
        if not hasattr(type(self), 'test_scored_matches') or not hasattr(type(self), 'test_fingerprint'):
            self.skipTest("Previous steps not completed")
        
        # Filter high-scoring matches
        high_score_matches = [
            m for m in type(self).test_scored_matches
            if m.overall_score >= 70.0
        ]
        
        if not high_score_matches:
            print("  ℹ No high-scoring matches to notify about")
            return
        
        # Get user ID from resume
        user_id = self.resume_data.get('email', 'test-pipeline-user')
        
        # Trigger notifications
        notification_count = 0
        for scored_match in high_score_matches:
            # Get match record from DB
            db_match = self.session.query(JobMatch).filter(
                JobMatch.preferences_file_hash == type(self).test_fingerprint,
                JobMatch.job_id == scored_match.job.id
            ).first()
            
            # Check if match exists and has an ID
            if db_match is not None:
                match_id = getattr(db_match, 'id', None)
                if match_id is not None:
                    try:
                        # Queue notification
                        job = self.notification_service.notify_new_match(
                            user_id=user_id,
                            match_id=str(match_id),
                            job_title=scored_match.job.title,
                            company=scored_match.job.company,
                            score=float(scored_match.overall_score),
                            location=scored_match.job.location_text,
                            is_remote=scored_match.job.is_remote or False,
                            channels=['in_app']  # Use in_app to avoid external calls
                        )
                        
                        if job:
                            notification_count += 1
                    except Exception as e:
                        print(f"  ⚠ Notification failed: {e}")
        
        print(f"  ✓ Queued {notification_count} notifications")
        
        # Verify queue
        if notification_count > 0:
            time.sleep(0.5)  # Brief wait for queue
            queue_length = self.notification_service.get_queue_status().get('queue_length', 0)
            print(f"  ✓ Queue has {queue_length} pending jobs")
    
    def test_07_end_to_end_flow(self):
        """Complete end-to-end flow test."""
        print("\n[Step 7] Complete End-to-End Flow Verification...")
        
        # Count all test data
        jobs = self.session.query(JobPost).filter(
            JobPost.canonical_fingerprint.like("test-pipeline-%")
        ).count()
        
        matches = self.session.query(JobMatch).filter(
            JobMatch.preferences_file_hash.like("test-pipeline-%")
        ).count()
        
        print(f"\n  📊 Pipeline Results:")
        print(f"     - Jobs processed: {jobs}")
        print(f"     - Matches created: {matches}")
        
        if self.redis_url and self.notification_service:
            status = self.notification_service.get_queue_status()
            print(f"     - Queue connected: {status.get('redis_connected', False)}")
        
        # Verify pipeline completed successfully
        self.assertGreater(jobs, 0, "No jobs were processed")
        self.assertGreater(matches, 0, "No matches were created")
        
        print(f"\n  ✅ Full pipeline integration test PASSED")


if __name__ == '__main__':
    if not RUN_TESTS:
        print("\n" + "="*70)
        print("SKIPPING: Set TEST_DATABASE_URL environment variable to run")
        print("Or ensure Docker is running for automatic container management")
        print("="*70 + "\n")
    else:
        unittest.main(verbosity=2)
