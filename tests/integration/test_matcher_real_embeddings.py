#!/usr/bin/env python3
"""
Integration Test: Matcher Service with Real Embeddings

This test verifies the matcher service works correctly with real numpy array
embeddings from pgvector, which would catch bugs like:
- "The truth value of an array with more than one element is ambiguous"

Usage:
    # With automatic Docker containers:
    uv run python -m pytest tests/integration_test_matcher_real_embeddings.py -v
    
    # Or with existing database:
    TEST_DATABASE_URL=postgresql://user:pass@localhost:5432/jobscout \
        uv run python -m pytest tests/integration_test_matcher_real_embeddings.py -v

Requirements:
    - Docker must be available and running
    - psycopg2-binary: uv add --dev psycopg2-binary
"""

import unittest
import sys
import os
import numpy as np
from datetime import datetime
from typing import List, Dict, Any

# Check if we should run with Docker containers
USE_DOCKER = os.environ.get('USE_DOCKER_CONTAINERS', '1') == '1'
TEST_DATABASE_URL = os.environ.get('TEST_DATABASE_URL')

# Try to import container management
try:
    from tests.conftest_docker import postgres_container
    DOCKER_AVAILABLE = True
except ImportError:
    DOCKER_AVAILABLE = False

# Determine if we can run
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

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from database.models import (
    Base, JobPost, JobRequirementUnit, JobRequirementUnitEmbedding
)
from database.repository import JobRepository
from core.matcher import MatcherService
from etl.resume import ResumeProfiler, ResumeEvidenceUnit
from core.config_loader import MatcherConfig
from core.llm.interfaces import LLMProvider


class MockAIService(LLMProvider):
    """Mock AI service that implements LLMProvider interface."""
    
    def extract_structured_data(self, text: str, schema: Dict) -> Dict[str, Any]:
        """Mock structured data extraction."""
        return {}
    
    def generate_embedding(self, text: str) -> List[float]:
        """Generate a random embedding for testing."""
        return np.random.randn(1024).tolist()
    
    def extract_job_facets(self, text: str) -> Dict[str, str]:
        """Mock facet extraction."""
        return {
            "remote_flexibility": "Remote work available",
            "compensation": "Competitive salary",
            "learning_growth": "Learning budget provided",
            "company_culture": "Great team culture",
            "work_life_balance": "Good work-life balance",
            "tech_stack": "Python, PostgreSQL, AWS",
            "visa_sponsorship": "Visa sponsorship available"
        }


class TestMatcherRealEmbeddings(unittest.TestCase):
    """
    Integration tests for MatcherService with real numpy array embeddings.
    
    These tests verify the matcher works correctly with actual pgvector embeddings,
    which are numpy arrays. This catches bugs like boolean checks on arrays.
    """
    
    @classmethod
    def setUpClass(cls):
        """Set up test database with real embeddings (either Docker or external)."""
        print("\n" + "="*70)
        print("INTEGRATION TEST: Matcher Service with Real Embeddings")
        print("="*70)
        
        if USE_EXTERNAL_DB:
            print(f"Using external database: {TEST_DATABASE_URL[:30]}...")
            cls.engine = create_engine(TEST_DATABASE_URL)
            cls._setup_database()
        else:
            print("Starting PostgreSQL Docker container...")
            try:
                cls.container_mgr = postgres_container()
                cls.container = cls.container_mgr.__enter__()
                print(f"✓ Container started on port {cls.container.host_port}")
                
                # Connect to container
                cls.engine = create_engine(cls.container.database_url)
                cls._setup_database()
            except Exception as e:
                print(f"✗ Failed to start container: {e}")
                raise
        
        # Create session
        SessionLocal = sessionmaker(bind=cls.engine)
        cls.session = SessionLocal()
        cls.repo = JobRepository(cls.session)
        
        # Mock AI service
        cls.mock_ai = MockAIService()

        # Create ResumeProfiler
        cls.resume_profiler = ResumeProfiler(ai_service=cls.mock_ai)

        # Create matcher
        cls.config = MatcherConfig(
            similarity_threshold=0.5,
            batch_size=10,
            include_job_level_matching=True
        )
        cls.matcher = MatcherService(cls.resume_profiler, cls.config)
        
        # Create test data
        cls._create_test_data()
        
        print("✓ Test database initialized with real embeddings")
    
    @classmethod
    def _setup_database(cls):
        """Create tables and enable pgvector extension."""
        # Enable pgvector extension first
        from sqlalchemy import text
        with cls.engine.connect() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
            conn.commit()
        # Then create all tables
        Base.metadata.create_all(cls.engine)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up."""
        if hasattr(cls, 'session'):
            cls.session.close()
        if hasattr(cls, 'engine'):
            cls.engine.dispose()
        
        # Stop container if we started it
        if not USE_EXTERNAL_DB and hasattr(cls, 'container_mgr'):
            print("\nStopping PostgreSQL container...")
            cls.container_mgr.__exit__(None, None, None)
        
        print("\n✓ Integration test complete")
        print("="*70 + "\n")
    
    @classmethod
    def _create_test_data(cls):
        """Create test jobs with real numpy array embeddings."""
        import uuid
        
        # Create test job with proper schema
        job_id = uuid.uuid4()
        job = JobPost(
            id=job_id,
            title="Python Developer",
            company="TestCorp",
            location_text="Tokyo",
            is_remote=True,
            description="Build Python microservices",
            status="active",
            canonical_fingerprint="test-fingerprint-001",
            raw_payload={"test": "data"},
            summary_embedding=np.random.randn(1024)  # Real numpy array!
        )
        
        # Create requirements with proper schema
        req1_id = uuid.uuid4()
        req1 = JobRequirementUnit(
            id=req1_id,
            job_post_id=job_id,
            text="5+ years Python experience",
            req_type="required",
            tags={"label": "experience_years"}
        )
        req1_embedding = JobRequirementUnitEmbedding(
            job_requirement_unit_id=req1_id,
            embedding=np.random.randn(1024)  # Real numpy array!
        )
        
        req2_id = uuid.uuid4()
        req2 = JobRequirementUnit(
            id=req2_id,
            job_post_id=job_id,
            text="AWS and Kubernetes",
            req_type="required",
            tags={"label": "skill"}
        )
        req2_embedding = JobRequirementUnitEmbedding(
            job_requirement_unit_id=req2_id,
            embedding=np.random.randn(1024)
        )
        
        cls.session.add(job)
        cls.session.add(req1)
        cls.session.add(req1_embedding)
        cls.session.add(req2)
        cls.session.add(req2_embedding)
        cls.session.commit()
        
        cls.test_job_id = job_id
    
    def test_01_job_with_summary_embedding_boolean_check(self):
        """Test job.summary_embedding boolean check works with numpy arrays."""
        print("\n[Test 1] Job summary_embedding boolean check...")
        
        job = self.session.query(JobPost).filter_by(id=self.test_job_id).first()
        
        self.assertIsNotNone(job.summary_embedding)
        self.assertIsInstance(job.summary_embedding, np.ndarray)
        print(f"  ✓ Job has numpy array embedding: shape={job.summary_embedding.shape}")
        
        try:
            result = job.summary_embedding is not None
            self.assertTrue(result)
            print("  ✓ Boolean check 'is not None' works correctly")
        except ValueError as e:
            self.fail(f"Boolean check failed: {e}")
    
    def test_02_requirement_embedding_boolean_check(self):
        """Test requirement embedding boolean check works with numpy arrays."""
        print("\n[Test 2] Requirement embedding boolean check...")
        
        # Query requirements by job_post_id since we store that as class variable
        req = self.session.query(JobRequirementUnit).filter_by(job_post_id=self.test_job_id).first()
        self.assertIsNotNone(req, "Requirement should exist")
        
        # Get the embedding row for this requirement
        embedding_row = self.session.query(JobRequirementUnitEmbedding).filter_by(
            job_requirement_unit_id=req.id
        ).first()
        self.assertIsNotNone(embedding_row, "Embedding row should exist")
        
        self.assertIsNotNone(embedding_row.embedding)
        self.assertIsInstance(embedding_row.embedding, np.ndarray)
        print(f"  ✓ Requirement has numpy array embedding: shape={embedding_row.embedding.shape}")
        
        try:
            result = embedding_row.embedding is not None
            self.assertTrue(result)
            print("  ✓ Boolean check 'is not None' works correctly")
        except ValueError as e:
            self.fail(f"Boolean check failed: {e}")
    
    def test_03_evidence_embedding_boolean_check(self):
        """Test evidence unit embedding boolean check."""
        print("\n[Test 3] Evidence embedding boolean check...")
        
        evidence = ResumeEvidenceUnit(
            id="reu_001",
            text="Python development",
            source_section="skills",
            tags={"type": "skill"},
            embedding=np.random.randn(1024).tolist()
        )
        
        self.assertIsNotNone(evidence.embedding)
        self.assertIsInstance(evidence.embedding, list)
        print(f"  ✓ Evidence has embedding: length={len(evidence.embedding)}")
        
        try:
            result = evidence.embedding is None
            self.assertFalse(result)
            print("  ✓ Boolean check 'is None' works correctly")
        except ValueError as e:
            self.fail(f"Boolean check failed: {e}")
    
    def test_04_matcher_service_with_real_embeddings(self):
        """Full matcher service test with real embeddings."""
        print("\n[Test 4] Full matcher service with real embeddings...")
        
        evidence_units = [
            ResumeEvidenceUnit(
                id="reu_001",
                text="Python development",
                source_section="skills",
                tags={"type": "skill"},
                embedding=np.random.randn(1024).tolist()
            ),
            ResumeEvidenceUnit(
                id="reu_002",
                text="AWS cloud infrastructure",
                source_section="skills",
                tags={"type": "skill"},
                embedding=np.random.randn(1024).tolist()
            ),
            ResumeEvidenceUnit(
                id="reu_003",
                text="5 years of experience",
                source_section="experience",
                tags={"type": "experience"},
                embedding=np.random.randn(1024).tolist()
            )
        ]
        
        job = self.session.query(JobPost).filter_by(id=self.test_job_id).first()
        
        try:
            from database.repository import JobRepository
            repo = JobRepository(self.session)
            result = self.matcher.match_resume_to_job(
                repo=repo,
                job=job,
                resume_fingerprint="test-fingerprint-123",
            )
            
            self.assertIsNotNone(result)
            print(f"  ✓ Matcher returned result")
            
        except ValueError as e:
            if "truth value of an array" in str(e):
                self.fail(f"Numpy array boolean error: {e}")
            else:
                raise


if __name__ == '__main__':
    unittest.main(verbosity=2)
