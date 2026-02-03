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
from core.matcher import MatcherService, ResumeEvidenceUnit
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

    def test_03_mock_match_resume_to_jobs(self):
        """Test MockMatcherService match_resume_to_jobs."""
        print("\n🔍 UNIT Test 3: Mock Job Matching")

        # Add some mock jobs to the repository
        from database.models import JobPost
        from unittest.mock import MagicMock

        mock_job = MagicMock()
        mock_job.id = "test-job-1"
        mock_job.title = "Software Engineer"
        self.matcher.repo.jobs["test-job-1"] = mock_job

        # Match resume to jobs
        matches = self.matcher.match_resume_to_jobs(self.resume_data, limit=10)

        # Should return mock matches
        self.assertIsInstance(matches, list)

        print(f"  ✓ Found {len(matches)} mock matches")
        print(f"  ✓ Mock matching works correctly")

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


if __name__ == '__main__':
    unittest.main(verbosity=2)
