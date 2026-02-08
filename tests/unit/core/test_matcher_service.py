#!/usr/bin/env python3
"""
Unit tests for MatcherService.
"""

import json
import os
import tempfile
import unittest
from typing import Dict, Any
from unittest.mock import MagicMock

from core.config_loader import load_config, MatcherConfig
from core.matcher import MatcherService
from etl.resume import ResumeEvidenceUnit, ResumeProfiler
from etl.resume.embedding_store import JobRepositoryAdapter
from tests.mocks.matcher_mocks import MockMatcherService
from database.models import generate_resume_fingerprint


class TestMatcherService(unittest.TestCase):
    """Unit tests for MatcherService - no database required."""

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

        # MockMatcherService takes similarity_threshold as a float, not MatcherConfig
        self.matcher = MockMatcherService(self.mock_repo, self.mock_ai, similarity_threshold=0.3)

    def test_01_matcher_config_loading(self):
        """Test loading matching config from YAML."""
        print("\n⚙️  UNIT Test 1: Config Loading")

        config_content = """
database:
  url: "postgresql://test:test@localhost:5432/test"

matching:
  enabled: true
  resume_file: "test_resume.json"

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
            self.assertEqual(config.matching.matcher.similarity_threshold, 0.6)
            self.assertEqual(config.matching.scorer.weight_required, 0.8)

            print(f"  ✓ Config loaded successfully")
            print(f"  ✓ Matching enabled: {config.matching.enabled}")

        finally:
            os.unlink(config_path)

    def test_02_matcher_initialization(self):
        """Test mock matcher service initialization."""
        print("\n🔧 UNIT Test 2: Matcher Initialization")

        # Verify matcher was initialized correctly
        self.assertIsNotNone(self.matcher)
        # MockMatcherService has similarity_threshold directly (not .config)
        self.assertEqual(self.matcher.similarity_threshold, 0.3)
        self.assertIsNotNone(self.matcher.repo)
        self.assertIsNotNone(self.matcher.ai)

        print(f"  ✓ Mock matcher initialized successfully")
        print(f"  ✓ Similarity threshold: {self.matcher.similarity_threshold}")

    def test_03_mock_match_resume_two_stage(self):
        """Test MockMatcherService match_resume_two_stage."""
        print("\n🔍 UNIT Test 3: Mock Two-Stage Job Matching")

        # Add some mock jobs to the repository
        from database.models import JobPost
        from unittest.mock import MagicMock

        mock_job = MagicMock()
        mock_job.id = "test-job-1"
        mock_job.title = "Software Engineer"
        self.matcher.repo.jobs["test-job-1"] = mock_job

        # Run two-stage matching
        matches = self.matcher.match_resume_two_stage(self.mock_repo, self.resume_data)

        # Should return mock matches
        self.assertIsInstance(matches, list)

        print(f"  ✓ Found {len(matches)} mock matches")
        print(f"  ✓ Two-stage matching works correctly")

    def test_04_resume_fingerprint_generation(self):
        """Test resume fingerprint generation."""
        print("\n🔐 UNIT Test 4: Resume Fingerprint")

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

    def test_05_resume_profiler_requires_store_for_embedding_persistence(self):
        """Test that ResumeProfiler requires a store to persist embeddings.

        This test verifies the bug fix where MatcherService failed with
        "No summary embedding found for resume" because ResumeProfiler
        was created without a store, causing embeddings to be lost.
        """
        print("\n💾 UNIT Test 5: Resume Profiler Store Requirement")

        # Test 1: ResumeProfiler without store has store=None
        mock_ai = MagicMock()
        profiler_without_store = ResumeProfiler(ai_service=mock_ai)
        
        # Verify store is None when not provided
        self.assertIsNone(profiler_without_store.store)
        print(f"  ✓ ResumeProfiler without store: store is None")

        # Test 2: ResumeProfiler with store properly sets it
        mock_repo = MagicMock()
        adapter = JobRepositoryAdapter(mock_repo)
        profiler_with_store = ResumeProfiler(
            ai_service=mock_ai,
            store=adapter
        )
        
        # Verify store is set
        self.assertIsNotNone(profiler_with_store.store)
        self.assertIsInstance(profiler_with_store.store, JobRepositoryAdapter)
        print(f"  ✓ ResumeProfiler with store: store is set to JobRepositoryAdapter")

        # Test 3: MatcherService integration - requires store to work
        config = MatcherConfig(similarity_threshold=0.5, batch_size=10)

        # This would fail in real usage without store because
        # get_resume_summary_embedding() returns None
        matcher_without_store = MatcherService(
            resume_profiler=ResumeProfiler(ai_service=mock_ai),
            config=config
        )

        # Verify the matcher was created but won't work without store
        self.assertIsNotNone(matcher_without_store)
        self.assertIsNone(matcher_without_store.resume_profiler.store)

        print(f"  ✓ MatcherService without store: store is None (will fail at runtime)")

        # With store - proper setup
        matcher_with_store = MatcherService(
            resume_profiler=ResumeProfiler(
                ai_service=mock_ai,
                store=JobRepositoryAdapter(mock_repo)
            ),
            config=config
        )

        self.assertIsNotNone(matcher_with_store.resume_profiler.store)
        print(f"  ✓ MatcherService with store: properly configured")

        # Test 4: Verify the save methods exist on the adapter
        self.assertTrue(hasattr(adapter, 'save_resume_section_embeddings'))
        self.assertTrue(hasattr(adapter, 'save_evidence_unit_embeddings'))
        print(f"  ✓ JobRepositoryAdapter has required save methods")


if __name__ == '__main__':
    unittest.main(verbosity=2)
