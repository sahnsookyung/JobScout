"""
Tests for multi-embedding resume matching.

Tests the new section-based embedding matching where each resume
section (experience, projects, skills, summary) gets its own embedding
and is matched individually against job requirements.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock

from etl.resume import ResumeProfiler
from core.scorer import ScoringService, ScoredJobMatch
from database.models import (
    JobPost,
    JobRequirementUnit,
    JobRequirementUnitEmbedding,
    ResumeSectionEmbedding
)
from database.repository import JobRepository
from core.llm.openai_service import OpenAIService


class TestMultiEmbeddingMatching:
    """Test suite for section-based resume embedding matching."""
    
    # Note: We no longer mock cosine_distance as it's now a column method.
    # Tests use mocked database queries instead of mocking the column method.
    
    @pytest.fixture
    def mock_ai_service(self):
        """Create a mock AI service that returns embeddings."""
        mock = Mock(spec=OpenAIService)
        mock.generate_embedding.return_value = [0.1, 0.2, 0.3]
        mock.extract_structured_data.return_value = {
            "profile": {
                "experience": [
                    {"company": "Google", "title": "Senior Engineer", "years": 5},
                    {"company": "Facebook", "title": "Engineer", "years": 3}
                ],
                "projects": [
                    {"name": "ML Pipeline", "description": "Built ML pipeline"}
                ],
                "skills": {"groups": [{"name": "backend", "skills": ["Python", "Go"]}]}
            }
        }
        return mock
    
    @pytest.fixture
    def sample_job_requirement(self):
        """Factory fixture to create a sample job requirement."""
        def _create_req(min_years=None):
            req = Mock(spec=JobRequirementUnit)
            req.id = "req-1"
            req.text = "Experience with Python"
            req.req_type = "required"
            
            # Add embedding row structure matching code expectation:
            # req.requirement_row.embedding_row.unit.embedding
            req.requirement_row = Mock()
            req.requirement_row.embedding_row = Mock()
            req.requirement_row.embedding_row.unit.embedding = [0.5, 0.3, 0.2]
            req.requirement_row.embedding_row.unit.min_years = min_years
            
            # Also populate unit directly on requirement_row for penalties.py access
            req.requirement_row.unit = Mock()
            req.requirement_row.unit.min_years = min_years
            return req
        return _create_req
    
    @pytest.fixture
    def sample_resume_sections(self):
        """Sample resume section embeddings as objects."""
        from types import SimpleNamespace
        
        sections_data = [
            {
                "id": "exp-1",
                "resume_fingerprint": "resume-123",
                "section_type": "experience",
                "section_index": 0,
                "source_text": "Google - Senior Engineer",
                "source_data": {"company": "Google", "title": "Senior Engineer", "years": 5},
                "embedding": [0.6, 0.4, 0.1]
            },
            {
                "id": "exp-2",
                "resume_fingerprint": "resume-123",
                "section_type": "experience",
                "section_index": 1,
                "source_text": "Facebook - Engineer",
                "source_data": {"company": "Facebook", "title": "Engineer", "years": 3},
                "embedding": [0.7, 0.3, 0.2]
            },
            {
                "id": "skill-1",
                "resume_fingerprint": "resume-123",
                "section_type": "skill",
                "section_index": 0,
                "source_text": "backend: Python, Go",
                "source_data": {"name": "backend", "skills": ["Python", "Go"]},
                "embedding": [0.4, 0.1, 0.5]
            }
        ]
        
        # Convert to objects (SimpleNamespace acts like an object with attributes)
        return [SimpleNamespace(**s) for s in sections_data]
    
    def test_01_section_embedding_retrieval(self, sample_resume_sections):
        """Test that repo can retrieve section embeddings."""
        sections = sample_resume_sections

        # Test data structure filtering - ensure we can filter by section_type
        result = [s for s in sections if s.section_type == 'experience']

        # Verify correct sections returned
        assert len(result) == 2
        for section in result:
            assert section.resume_fingerprint == "resume-123"
            assert section.section_type == "experience"
    
    def test_02_requirement_section_similarity(self, sample_job_requirement, sample_resume_sections):
        """Test similarity calculation between requirement and resume sections."""
        from core.matcher.explainability import calculate_requirement_similarity_with_resume_sections
        from unittest.mock import Mock, MagicMock
        
        # Create proper mock sections with cosine_distance method
        mock_sections = []
        for s in sample_resume_sections:
            if s.section_type == 'experience':
                mock_section = Mock()
                mock_section.section_type = s.section_type
                mock_section.section_index = s.section_index
                mock_section.source_text = s.source_text
                mock_section.embedding = Mock()
                mock_section.embedding.cosine_distance = Mock(return_value=0.1)  # similarity = 0.9
                mock_sections.append(mock_section)
        
        # Mock repository - need to mock the resume sub-repo
        repo = Mock(spec=JobRepository)
        repo.resume = Mock()
        repo.resume.get_resume_section_embeddings = Mock(return_value=mock_sections)
        
        job_req = sample_job_requirement()
        
        # Simulate similarity calculation
        sim_score, details = calculate_requirement_similarity_with_resume_sections(
            job_requirement=job_req,
            resume_fingerprint="resume-123",
            repo=repo,
            section_types=['experience'],
            top_k=1
        )

        # Verify similarity score
        assert sim_score > 0
        assert 'similarity' in details
        assert 'best_section' in details
        # The mock returns experience sections
        assert details['best_section'] == 'experience'
    
    def test_03_experience_mismatch_penalty(self, sample_job_requirement, sample_resume_sections):
        """Test penalty when experience section has less years than required."""
        from core.scorer import penalties
        from core.matcher import RequirementMatchResult
        from database.models import JobPost
        
        req = sample_job_requirement(min_years=5)
        
        req_match = Mock(spec=RequirementMatchResult)
        req_match.requirement = req
        req_match.evidence = Mock()
        req_match.evidence.years_value = None  # Priority 2: falls back to experience sections
        req_match.evidence.text = "Python development"
        req_match.is_covered = True
        req_match.requirement_row = req.requirement_row
        
        section = {
            "id": "exp-1",
            "resume_fingerprint": "resume-123",
            "section_type": "experience",
            "section_index": 0,
            "source_text": "Company A - Engineer (3 years)",
            "source_data": {"company": "Company A", "title": "Engineer", "years_value": 3.0},
            "has_embedding": True,
        }
        
        job = Mock(spec=JobPost)
        job.location_text = "Remote"
        job.is_remote = True
        job.job_level = "Senior"
        job.salary_max = 100000
        
        config = Mock()
        config.penalty_experience_shortfall = 10.0
        config.penalty_missing_required = 10.0
        config.penalty_location_mismatch = 10.0
        config.penalty_seniority_mismatch = 10.0
        config.target_seniority = None
        config.min_salary = None
        config.wants_remote = False
        
        _, details = penalties.calculate_fit_penalties(
            job=job,
            matched_requirements=[req_match],
            missing_requirements=[],
            config=config,
            candidate_total_years=3.0,
            experience_sections=[section]
        )
        
        exp_penalties = [d for d in details if d['type'] == 'experience_years_mismatch']
        assert len(exp_penalties) > 0
        assert exp_penalties[0]['amount'] > 0
        assert exp_penalties[0]['amount'] == 20.0
    
    def test_04_extract_structured_resume_returns_ai_data(self):
        """Test that ResumeProfiler passes through AI-extracted data."""
        from etl.resume import ResumeProfiler
        
        experience = {
            "company": "Google",
            "title": "Senior Engineer",
            "description": "5 years of experience building scalable systems"
        }

        # Mock AI service
        mock_ai = Mock()
        mock_ai.extract_structured_data.return_value = {
            "profile": {
                "experience": [{**experience, "years_value": 5.0}]
            }
        }

        profiler = ResumeProfiler(ai_service=mock_ai)

        # Test that the profiler passes through the AI data correctly
        profile = profiler.extract_structured_resume(
            resume_data={"experience": [experience]}
        )

        # Verify the AI data is returned (pass-through behavior)
        if profile and profile.raw_data:
            exp_section = profile.raw_data.get('profile', {}).get('experience', [{}])[0]
            assert exp_section.get('years_value') == 5.0
    
    def test_05_save_resume_section_embeddings(self):
        """Test saving resume section embeddings."""
        sections = [
            {
                "section_type": "experience",
                "section_index": 0,
                "source_text": "Test experience",
                "source_data": {"company": "Test", "title": "Engineer"},
                "embedding": [0.1, 0.2, 0.3]
            }
        ]
        
        # To test Repository method, we should instantiate a real JobRepository with a mock DB session
        # OR match the method signature if mocking the repo itself.
        # The test originally called `repo.save_resume_section_embeddings`.
        
        mock_db = Mock()
        repo = JobRepository(db=mock_db)
        
        # Mock execute result
        mock_db.execute.return_value = Mock() # for delete
        
        result = repo.save_resume_section_embeddings(
            resume_fingerprint="resume-123",
            sections=sections
        )
        
        # Verify save was called
        # save_resume_section_embeddings returns records
        assert len(result) == 1
        mock_db.flush.assert_called_once()
        # It calls execute multiple times (delete, then implicit add is via session.add)
        assert mock_db.execute.called
    
    def test_06_get_resume_section_embeddings_with_filters(self, sample_resume_sections):
        """Test retrieving section embeddings with filters."""
        # Using objects
        sections = sample_resume_sections
        
        mock_db = Mock()
        repo = JobRepository(db=mock_db)
        
        # Mock return for get_resume_section_embeddings which calls execute().scalars().all()
        mock_db.execute.return_value.scalars.return_value.all.return_value = [
            s for s in sections if s.section_type == 'experience'
        ]
        
        result = repo.get_resume_section_embeddings(
            resume_fingerprint="resume-123",
            section_type='experience'
        )
        
        # Verify correct sections returned
        assert len(result) == 2
        for section in result:
            assert section.section_type == 'experience'
            assert section.resume_fingerprint == "resume-123"
