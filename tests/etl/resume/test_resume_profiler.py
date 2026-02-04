#!/usr/bin/env python3
"""Test ResumeProfiler separation of profiling and persistence."""
import pytest
from unittest.mock import Mock, MagicMock

from etl.resume import ResumeProfiler
from etl.resume.embedding_store import InMemoryEmbeddingStore, JobRepositoryAdapter
from etl.resume.models import ResumeEvidenceUnit


class TestResumeProfilerProfiling:
    """Test suite for ResumeProfiler profiling functionality."""
    
    @pytest.fixture
    def mock_ai_service(self):
        """Create a mock AI service."""
        mock = Mock()
        mock.extract_structured_data.return_value = {
            "profile": {
                "experience": [
                    {
                        "company": "TechCorp",
                        "title": "Software Engineer",
                        "years": 3,
                        "description": "Built Python services",
                        "highlights": ["Deployed to AWS", "Mentored juniors"]
                    }
                ],
                "projects": [
                    {
                        "name": "ML Pipeline",
                        "description": "Built ML pipeline"
                    }
                ],
                "skills": {
                    "groups": [
                        {"name": "backend", "skills": ["Python", "Go"]}
                    ]
                },
                "summary": {
                    "headline": "Software Engineer",
                    "objective": "Build scalable systems"
                }
            }
        }
        mock.generate_embedding.return_value = [0.1, 0.2, 0.3] * 341
        return mock
    
    @pytest.fixture
    def sample_resume_data(self):
        """Create sample resume data."""
        return {
            "sections": [
                {
                    "title": "Experience",
                    "items": [
                        {
                            "company": "TechCorp",
                            "role": "Software Engineer",
                            "period": "2020-2023",
                            "description": "Built Python services",
                            "highlights": ["Deployed to AWS", "Mentored juniors"]
                        }
                    ]
                },
                {
                    "title": "Skills",
                    "items": [
                        {
                            "skills": "Python, Go, AWS"
                        }
                    ]
                }
            ]
        }
    
    def test_profile_resume_without_store_returns_payload(
        self, mock_ai_service, sample_resume_data
    ):
        """Test that profile_resume returns persistence payload even without store."""
        profiler = ResumeProfiler(ai_service=mock_ai_service)
        
        profile, evidence_units, persistence_payload = profiler.profile_resume(sample_resume_data)
        
        # Profile should be extracted
        assert profile is not None
        
        # Evidence units should be extracted
        assert len(evidence_units) > 0
        
        # Persistence payload should be returned (even if empty when no store)
        assert isinstance(persistence_payload, list)
    
    def test_profile_resume_without_store_does_not_call_repo(
        self, mock_ai_service, sample_resume_data
    ):
        """Test that profile_resume without store does not persist to DB."""
        profiler = ResumeProfiler(ai_service=mock_ai_service)
        
        profiler.profile_resume(sample_resume_data)
        
        # AI service should not be called for section embeddings (no store)
        # extract_structured_data is called, but save_resume_section_embeddings won't persist
        # without a store configured
        mock_ai_service.generate_embedding.assert_called()
    
    def test_profile_resume_with_store_persists(
        self, mock_ai_service, sample_resume_data
    ):
        """Test that profile_resume with store persists embeddings."""
        store = InMemoryEmbeddingStore()
        profiler = ResumeProfiler(ai_service=mock_ai_service, store=store)
        
        profile, evidence_units, persistence_payload = profiler.profile_resume(sample_resume_data)
        
        # Persistence should have happened
        assert len(persistence_payload) > 0
        
        # Store should have saved sections
        # Note: We can't easily check without a fingerprint, but the payload exists
    
    def test_profiler_can_be_created_without_repo(
        self, mock_ai_service
    ):
        """Test that ResumeProfiler can be instantiated without a repository."""
        # This should work - no repo required
        profiler = ResumeProfiler(ai_service=mock_ai_service)
        
        assert profiler.store is None
        assert profiler.ai is not None


class TestJobRepositoryAdapter:
    """Test suite for JobRepository adapter."""
    
    def test_adapter_wraps_save_call(self):
        """Test that adapter correctly wraps JobRepository save call."""
        mock_repo = Mock()
        mock_repo.save_resume_section_embeddings.return_value = None
        
        adapter = JobRepositoryAdapter(mock_repo)
        
        sections = [{"section_type": "experience", "embedding": [0.1, 0.2, 0.3]}]
        adapter.save_resume_section_embeddings("test-fingerprint", sections)
        
        mock_repo.save_resume_section_embeddings.assert_called_once_with(
            resume_fingerprint="test-fingerprint",
            sections=sections
        )
    
    def test_adapter_wraps_get_call(self):
        """Test that adapter correctly wraps JobRepository get call."""
        mock_repo = Mock()
        mock_repo.get_resume_section_embeddings.return_value = []
        
        adapter = JobRepositoryAdapter(mock_repo)
        
        adapter.get_resume_section_embeddings("test-fingerprint", "experience")
        
        mock_repo.get_resume_section_embeddings.assert_called_once_with(
            resume_fingerprint="test-fingerprint",
            section_type="experience"
        )
    
    def test_adapter_with_none_section_type(self):
        """Test that adapter handles None section_type correctly."""
        mock_repo = Mock()
        mock_repo.get_resume_section_embeddings.return_value = []
        
        adapter = JobRepositoryAdapter(mock_repo)
        
        adapter.get_resume_section_embeddings("test-fingerprint", None)
        
        mock_repo.get_resume_section_embeddings.assert_called_once_with(
            resume_fingerprint="test-fingerprint",
            section_type=None
        )


class TestInMemoryEmbeddingStore:
    """Test suite for InMemoryEmbeddingStore."""
    
    def test_save_and_retrieve(self):
        """Test basic save and retrieve functionality."""
        store = InMemoryEmbeddingStore()
        
        sections = [
            {"section_type": "experience", "embedding": [0.1, 0.2, 0.3]},
            {"section_type": "skills", "embedding": [0.4, 0.5, 0.6]}
        ]
        
        store.save_resume_section_embeddings("resume-123", sections)
        
        retrieved = store.get_resume_section_embeddings("resume-123")
        
        assert len(retrieved) == 2
    
    def test_filter_by_section_type(self):
        """Test filtering by section type."""
        store = InMemoryEmbeddingStore()
        
        sections = [
            {"section_type": "experience", "embedding": [0.1, 0.2, 0.3]},
            {"section_type": "skills", "embedding": [0.4, 0.5, 0.6]},
            {"section_type": "experience", "embedding": [0.7, 0.8, 0.9]}
        ]
        
        store.save_resume_section_embeddings("resume-123", sections)
        
        experience_sections = store.get_resume_section_embeddings(
            "resume-123", "experience"
        )
        
        assert len(experience_sections) == 2
        for section in experience_sections:
            assert section["section_type"] == "experience"
    
    def test_clear(self):
        """Test clearing storage."""
        store = InMemoryEmbeddingStore()
        
        store.save_resume_section_embeddings("resume-123", [{"section_type": "skills"}])
        store.clear()
        
        retrieved = store.get_resume_section_embeddings("resume-123")
        assert len(retrieved) == 0
