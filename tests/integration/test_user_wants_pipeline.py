#!/usr/bin/env python3
"""Integration Test: User-Wants Scoring Pipeline

Validates complete user-wants feature with exact score calculations:
- File loading → embeddings → matching → scoring → results
- PostgreSQL Docker container with clean DB
- Deterministic embeddings (hash-based RNG)
- 1e-9 precision tolerance
"""

import json
import sys
import unittest
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from core.config_loader import FacetWeights, ScorerConfig
from core.matcher import JobMatchPreliminary
from core.llm.interfaces import LLMProvider
from core.scorer import ScoringService
from database.models import Base, JobPost
from database.repository import JobRepository
from etl.resume import ResumeProfiler
from etl.resume.embedding_store import JobRepositoryAdapter


import hashlib


class DeterministicMockAIService(LLMProvider):
    """Mock AI with isolated RNG for deterministic embeddings."""
    
    def __init__(self, dim: int = 1024):
        self.dim = dim
    
    def generate_embedding(self, text: str) -> List[float]:
        # Use stable hash instead of Python's hash() which varies by session
        text_hash = int(hashlib.md5(text.encode()).hexdigest(), 16) % (2**32)
        rng = np.random.default_rng(text_hash)
        emb = rng.standard_normal(self.dim)
        return (emb / np.linalg.norm(emb)).astype(np.float32).tolist()
    
    def extract_structured_data(self, text: str, schema_spec: dict, *args, **kwargs) -> dict:
        return {}
    
    def extract_resume_data(self, text: str) -> dict:
        return {}
    
    def extract_requirements_data(self, text: str) -> dict:
        return {"required": [], "preferred": []}
    
    def extract_facet_data(self, text: str) -> Dict[str, str]:
        text_lower = text.lower()
        facets = {
            "remote_flexibility": "Standard office environment",
            "compensation": "Standard benefits",
            "learning_growth": "Training available",
            "company_culture": "Professional environment",
            "work_life_balance": "Standard hours",
            "tech_stack": "Common technologies",
            "visa_sponsorship": "No sponsorship"
        }
        if "remote" in text_lower:
            facets["remote_flexibility"] = "Fully remote with flexible hours"
        if "python" in text_lower:
            facets["tech_stack"] = "Python, FastAPI, PostgreSQL, AWS"
        elif "java" in text_lower:
            facets["tech_stack"] = "Java, Spring Boot, Oracle"
        return facets


class TestUserWantsPipelineIntegration(unittest.TestCase):
    """Test user-wants scoring with exact calculations."""
    
    TOLERANCE = 1e-9
    
    @classmethod
    def setUpClass(cls):
        """Setup PostgreSQL and load test data from JSON."""
        print("\n" + "="*60)
        print("User-Wants Pipeline Integration Test")
        print("="*60)
        
        try:
            from tests.conftest_docker import postgres_container
        except ImportError:
            raise unittest.SkipTest("Docker not available")
        
        # Start PostgreSQL
        cls.postgres_mgr = postgres_container(host_port=15433)
        cls.postgres = cls.postgres_mgr.__enter__()
        print(f"✓ PostgreSQL: {cls.postgres.database_url[:40]}...")
        
        # Setup database
        cls.engine = create_engine(cls.postgres.database_url)
        with cls.engine.connect() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
            conn.commit()
        Base.metadata.create_all(cls.engine)
        
        # Create session and services
        Session = sessionmaker(bind=cls.engine)
        cls.session = Session()
        cls.repo = JobRepository(cls.session)
        cls.mock_ai = DeterministicMockAIService()
        cls.resume_profiler = ResumeProfiler(
            ai_service=cls.mock_ai,
            store=JobRepositoryAdapter(cls.repo)
        )
        
        # Load test data from JSON
        cls._load_test_data()
        print("✓ Test data loaded\n")
    
    @classmethod
    def tearDownClass(cls):
        """Cleanup database and containers."""
        if hasattr(cls, 'session'):
            cls.session.query(JobPost).filter(
                JobPost.canonical_fingerprint.like("test-wants-%")
            ).delete(synchronize_session=False)
            cls.session.commit()
            cls.session.close()
        
        if hasattr(cls, 'engine'):
            cls.engine.dispose()
        
        if hasattr(cls, 'postgres_mgr'):
            cls.postgres_mgr.__exit__(None, None, None)
        
        print("="*60 + "\n")
    
    @classmethod
    def _load_test_data(cls):
        """Load jobs and user wants from JSON fixture."""
        fixture_path = Path(__file__).parent.parent / "fixtures" / "user_wants" / "test_jobs.json"
        with open(fixture_path) as f:
            data = json.load(f)
        
        cls.jobs_data = data["jobs"]
        cls.user_wants_data = data["user_wants"]
        cls.test_job_ids = []
        
        for i, job in enumerate(cls.jobs_data):
            # Generate UUID from job ID string
            job_id = uuid.uuid5(uuid.NAMESPACE_DNS, job["id"])
            cls.test_job_ids.append(job_id)
            
            # Create job post
            post = JobPost(
                id=job_id,
                canonical_fingerprint=f"test-wants-{job_id}",
                title=job["title"],
                company=job["company"],
                location_text=job["location"],
                is_remote=job["is_remote"],
                description=job["description"],
                first_seen_at=datetime.now(),
                last_seen_at=datetime.now(),
                status="active",
                is_embedded=True,
                summary_embedding=cls.mock_ai.generate_embedding(job["description"]),
                raw_payload={"source": "test"}
            )
            cls.session.add(post)
            
            # Store facet embeddings
            for facet_key, facet_text in job["facets"].items():
                embedding = cls.mock_ai.generate_embedding(facet_text)
                cls.repo.save_job_facet_embedding(
                    job_post_id=job_id,
                    facet_key=facet_key,
                    facet_text=facet_text,
                    embedding=embedding,
                    content_hash=f"hash-{facet_key}-{job_id}"
                )
        
        cls.session.commit()
    
    def _create_matches(self) -> List[JobMatchPreliminary]:
        """Create preliminary matches."""
        matches = []
        for i, job in enumerate(self.jobs_data):
            job_id = uuid.uuid5(uuid.NAMESPACE_DNS, job["id"])
            job_obj = self.session.query(JobPost).filter(
                JobPost.id == job_id
            ).first()
            if job_obj:
                is_python = "Python" in job_obj.title
                matches.append(JobMatchPreliminary(
                    job=job_obj,
                    job_similarity=0.75 if is_python else 0.55,
                    requirement_matches=[],
                    missing_requirements=[],
                    resume_fingerprint="test-resume-001"
                ))
        return matches
    
    def _get_facet_embeddings(self, matches: List[JobMatchPreliminary]) -> Dict[str, Dict]:
        """Get facet embeddings for all jobs."""
        embeddings = {}
        for pm in matches:
            job_id = str(pm.job.id)
            facets = self.repo.get_job_facet_embeddings(pm.job.id)
            embeddings[job_id] = {k: np.array(v, dtype=np.float32) for k, v in facets.items()}
        return embeddings
    
    def _generate_embeddings(self, texts: List[str]) -> List[np.ndarray]:
        """Generate embeddings for texts."""
        return [np.array(self.mock_ai.generate_embedding(t), dtype=np.float32) for t in texts]
    
    def _print_results(self, matches: List, title: str = "Results"):
        """Print test results."""
        print(f"\n{title}")
        print("-" * 50)
        for m in matches:
            print(f"  {m.job.title[:40]:<40}")
            print(f"    Fit: {m.fit_score:.2f}, Want: {m.want_score:.2f}, Overall: {m.overall_score:.2f}")
    
    def test_basic_score_calculation(self):
        """Verify overall = fit_weight * fit + want_weight * want."""
        scorer = ScoringService(self.repo, ScorerConfig(fit_weight=0.7, want_weight=0.3))
        matches = self._create_matches()
        wants = self._generate_embeddings(self.user_wants_data["basic"])
        facets = self._get_facet_embeddings(matches)
        
        scored = scorer.score_matches(
            preliminary_matches=matches,
            user_want_embeddings=wants,
            job_facet_embeddings_map=facets
        )
        
        self._print_results(scored, "[Test 1] Basic Score Calculation")
        
        for m in scored:
            expected = min(100.0, 0.7 * m.fit_score + 0.3 * m.want_score)
            self.assertAlmostEqual(m.overall_score, expected, delta=self.TOLERANCE)
            self.assertTrue(0 <= m.fit_score <= 100)
            self.assertTrue(0 <= m.want_score <= 100)
        
        print("  ✓ Calculations verified")
    
    def test_weight_configuration_impact(self):
        """Verify different fit/want weights produce correct results."""
        matches = self._create_matches()
        wants = self._generate_embeddings(self.user_wants_data["remote_focus"])
        facets = self._get_facet_embeddings(matches)
        
        # Test 50/50 weights
        scorer_50_50 = ScoringService(self.repo, ScorerConfig(fit_weight=0.5, want_weight=0.5))
        scored_50_50 = scorer_50_50.score_matches(
            preliminary_matches=matches,
            user_want_embeddings=wants,
            job_facet_embeddings_map=facets
        )
        
        # Test 90/10 weights
        scorer_90_10 = ScoringService(self.repo, ScorerConfig(fit_weight=0.9, want_weight=0.1))
        scored_90_10 = scorer_90_10.score_matches(
            preliminary_matches=matches,
            user_want_embeddings=wants,
            job_facet_embeddings_map=facets
        )
        
        print("\n[Test 2] Weight Configuration Impact")
        print("-" * 50)
        
        for m50, m90 in zip(scored_50_50, scored_90_10):
            print(f"  {m50.job.title[:40]:<40}")
            print(f"    50/50: {m50.overall_score:.2f}, 90/10: {m90.overall_score:.2f}")
            
            # Verify calculations (with cap at 100.0 for consistency)
            expected_50 = min(100.0, 0.5 * m50.fit_score + 0.5 * m50.want_score)
            expected_90 = min(100.0, 0.9 * m90.fit_score + 0.1 * m90.want_score)
            self.assertAlmostEqual(m50.overall_score, expected_50, delta=self.TOLERANCE)
            self.assertAlmostEqual(m90.overall_score, expected_90, delta=self.TOLERANCE)
            
            # Fit and want should be identical (same inputs)
            self.assertAlmostEqual(m50.fit_score, m90.fit_score, delta=self.TOLERANCE)
            self.assertAlmostEqual(m50.want_score, m90.want_score, delta=self.TOLERANCE)
        
        print("  ✓ Weight impact verified")
    
    def test_facet_weight_impact(self):
        """Verify changing facet weights affects want scores."""
        matches = self._create_matches()
        wants = self._generate_embeddings(self.user_wants_data["tech_focus"])
        facets = self._get_facet_embeddings(matches)
        
        # High remote weight config
        config_remote = ScorerConfig(facet_weights=FacetWeights(
            remote_flexibility=0.5, compensation=0.1, learning_growth=0.1,
            company_culture=0.1, work_life_balance=0.1, tech_stack=0.05, visa_sponsorship=0.05
        ))
        
        # High tech weight config
        config_tech = ScorerConfig(facet_weights=FacetWeights(
            remote_flexibility=0.1, compensation=0.1, learning_growth=0.1,
            company_culture=0.1, work_life_balance=0.1, tech_stack=0.4, visa_sponsorship=0.1
        ))
        
        scored_remote = ScoringService(self.repo, config_remote).score_matches(
            preliminary_matches=matches,
            user_want_embeddings=wants,
            job_facet_embeddings_map=facets
        )
        scored_tech = ScoringService(self.repo, config_tech).score_matches(
            preliminary_matches=matches,
            user_want_embeddings=wants,
            job_facet_embeddings_map=facets
        )
        
        print("\n[Test 3] Facet Weight Impact")
        print("-" * 50)
        
        differences = []
        for mr, mt in zip(scored_remote, scored_tech):
            diff = abs(mr.want_score - mt.want_score)
            differences.append(diff)
            print(f"  {mr.job.title[:40]:<40}")
            print(f"    Remote weights: {mr.want_score:.2f}, Tech weights: {mt.want_score:.2f}")
        
        # At least one job should show significant difference
        self.assertGreater(max(differences), 0.1, "Facet weight changes should impact scores")
        print(f"  Max difference: {max(differences):.2f}")
        print("  ✓ Facet weight impact verified")
    
    def test_fit_only_vs_fit_want_mode(self):
        """Verify fit-only mode (no wants) vs fit+want mode."""
        matches = self._create_matches()
        scorer = ScoringService(self.repo, ScorerConfig(fit_weight=0.7, want_weight=0.3))
        
        # Fit-only mode
        scored_fit_only = scorer.score_matches(preliminary_matches=matches)
        
        # Fit+want mode
        wants = self._generate_embeddings(self.user_wants_data["multiple"])
        facets = self._get_facet_embeddings(matches)
        scored_fit_want = scorer.score_matches(
            preliminary_matches=matches,
            user_want_embeddings=wants,
            job_facet_embeddings_map=facets
        )
        
        print("\n[Test 4] Fit-Only vs Fit+Want Mode")
        print("-" * 50)
        
        for m_only, m_want in zip(scored_fit_only, scored_fit_want):
            print(f"  {m_only.job.title[:40]:<40}")
            print(f"    Fit-only: Want={m_only.want_score:.2f}, Overall={m_only.overall_score:.2f}")
            print(f"    Fit+Want: Want={m_want.want_score:.2f}, Overall={m_want.overall_score:.2f}")
            
            # Fit-only: want_score = 0, overall = fit
            self.assertAlmostEqual(m_only.want_score, 0.0, delta=self.TOLERANCE)
            self.assertAlmostEqual(m_only.overall_score, m_only.fit_score, delta=self.TOLERANCE)
            
            # Fit+want: want_score > 0, overall = weighted
            self.assertGreater(m_want.want_score, 0.0)
            expected = min(100.0, 0.7 * m_want.fit_score + 0.3 * m_want.want_score)
            self.assertAlmostEqual(m_want.overall_score, expected, delta=self.TOLERANCE)
        
        print("  ✓ Mode switching verified")
    
    def test_empty_wants(self):
        """Verify empty wants list produces want_score = 0."""
        matches = self._create_matches()
        facets = self._get_facet_embeddings(matches)
        scorer = ScoringService(self.repo, ScorerConfig(fit_weight=0.7, want_weight=0.3))
        
        scored = scorer.score_matches(
            preliminary_matches=matches,
            user_want_embeddings=[],
            job_facet_embeddings_map=facets
        )
        
        print("\n[Test 5] Empty Wants Edge Case")
        print("-" * 50)
        
        for m in scored:
            print(f"  {m.job.title[:40]:<40} Want={m.want_score:.2f}, Overall={m.overall_score:.2f}")
            self.assertAlmostEqual(m.want_score, 0.0, delta=self.TOLERANCE)
            self.assertAlmostEqual(m.overall_score, m.fit_score, delta=self.TOLERANCE)
        
        print("  ✓ Empty wants verified")


if __name__ == '__main__':
    unittest.main()
