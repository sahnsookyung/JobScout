#!/usr/bin/env python3
"""Test RequirementMatcher using pgvector queries."""
import pytest
from unittest.mock import Mock, MagicMock

from core.matcher.requirement_matcher import RequirementMatcher
from etl.resume import ResumeEvidenceUnit
from database.models import JobRequirementUnit, ResumeEvidenceUnitEmbedding


@pytest.fixture
def mock_repo():
    """Create a mock repository with pgvector query support."""
    repo = Mock()
    repo.find_best_evidence_for_requirement.return_value = []
    return repo


@pytest.fixture
def sample_evidence_units():
    return [
        ResumeEvidenceUnit(
            id="reu_001",
            text="Python development",
            source_section="skills",
            tags={"type": "skill"},
            embedding=[0.8, 0.2, 0.1] * 341
        ),
        ResumeEvidenceUnit(
            id="reu_002",
            text="Other skill",
            source_section="skills",
            tags={"type": "skill"},
            embedding=[0.9, 0.8, 0.7] * 341
        ),
        ResumeEvidenceUnit(
            id="reu_003",
            text="AWS cloud",
            source_section="experience",
            tags={"company": "TechCorp", "type": "description"},
            embedding=[0.5, 0.3, 0.2] * 341
        )
    ]


@pytest.fixture
def mock_requirement_with_embedding():
    """Create a mock requirement with an embedding row."""
    req = Mock(spec=JobRequirementUnit)
    req.id = "req-001"
    req.text = "Python expertise"
    req.req_type = "required"

    embedding_row = Mock()
    embedding_row.embedding = [0.85, 0.15, 0.25] * 341
    req.embedding_row = embedding_row

    req.min_years = None
    return req


@pytest.fixture
def mock_requirement_no_embedding():
    """Create a mock requirement without an embedding."""
    req = Mock(spec=JobRequirementUnit)
    req.id = "req-002"
    req.text = "Python expertise"
    req.req_type = "required"
    req.embedding_row = None
    req.min_years = None
    return req


@pytest.fixture
def mock_evidence_row():
    """Create a mock ResumeEvidenceUnitEmbedding row."""
    row = Mock(spec=ResumeEvidenceUnitEmbedding)
    row.evidence_unit_id = "reu_001"
    row.source_text = "Python development"
    row.source_section = "Skills"
    row.tags = {"type": "skill"}
    row.embedding = [0.2, 0.5, 0.3] * 341
    row.years_value = None
    row.years_context = None
    row.is_total_years_claim = False
    return row


def test_requirement_matcher_covered(mock_requirement_with_embedding, mock_repo, mock_evidence_row):
    """Test RequirementMatcher matches evidence to requirements using pgvector."""
    mock_repo.find_best_evidence_for_requirement.return_value = [(mock_evidence_row, 0.8)]

    matcher = RequirementMatcher(
        similarity_threshold=0.5
    )

    matched, missing = matcher.match_requirements(
        mock_repo,
        [mock_requirement_with_embedding],
        resume_fingerprint="test-fingerprint"
    )

    assert len(matched) == 1
    assert len(missing) == 0
    assert matched[0].is_covered == True
    assert matched[0].similarity >= 0.5
    assert matched[0].evidence is not None


def test_requirement_matcher_not_covered(mock_requirement_no_embedding, mock_repo):
    """Test RequirementMatcher when requirement is not covered."""
    mock_repo.find_best_evidence_for_requirement.return_value = []

    matcher = RequirementMatcher(
        similarity_threshold=0.6
    )

    matched, missing = matcher.match_requirements(
        mock_repo,
        [mock_requirement_no_embedding],
        resume_fingerprint="test-fingerprint"
    )

    assert len(matched) == 0
    assert len(missing) == 1
    assert missing[0].is_covered == False
    assert missing[0].similarity == 0.0
    assert missing[0].evidence is None


def test_requirement_matcher_threshold(mock_requirement_with_embedding, mock_repo, mock_evidence_row):
    """Test similarity threshold is respected."""
    mock_repo.find_best_evidence_for_requirement.return_value = [(mock_evidence_row, 0.6)]

    matcher = RequirementMatcher(
        similarity_threshold=0.8
    )

    matched, missing = matcher.match_requirements(
        mock_repo,
        [mock_requirement_with_embedding],
        resume_fingerprint="test-fingerprint"
    )

    assert len(matched) == 0
    assert len(missing) == 1
    assert missing[0].is_covered == False
    assert missing[0].similarity < 0.8
    assert missing[0].evidence is not None


def test_requirement_embedding_exception(mock_requirement_no_embedding, mock_repo):
    """Test that requirement embedding failure creates missing requirement."""
    mock_repo.find_best_evidence_for_requirement.return_value = []

    matcher = RequirementMatcher(
        similarity_threshold=0.5
    )

    matched, missing = matcher.match_requirements(
        mock_repo,
        [mock_requirement_no_embedding],
        resume_fingerprint="test-fingerprint"
    )

    assert len(matched) == 0
    assert len(missing) == 1
    assert missing[0].similarity == 0.0
    assert missing[0].evidence is None
    assert missing[0].is_covered == False


def test_pgvector_query_called_correctly(mock_requirement_with_embedding, mock_repo, mock_evidence_row):
    """Test that pgvector query is called with correct parameters."""
    mock_repo.find_best_evidence_for_requirement.return_value = [(mock_evidence_row, 0.7)]

    matcher = RequirementMatcher(
        similarity_threshold=0.5
    )

    matcher.match_requirements(
        mock_repo,
        [mock_requirement_with_embedding],
        resume_fingerprint="test-fingerprint-123"
    )

    mock_repo.find_best_evidence_for_requirement.assert_called_once()
    call_args = mock_repo.find_best_evidence_for_requirement.call_args
    assert call_args[1]['resume_fingerprint'] == "test-fingerprint-123"
    assert call_args[1]['top_k'] == 1


def test_evidence_included_for_uncovered_requirements(mock_requirement_with_embedding, mock_repo, mock_evidence_row):
    """Test that best evidence is included even when below threshold."""
    mock_repo.find_best_evidence_for_requirement.return_value = [(mock_evidence_row, 0.4)]

    matcher = RequirementMatcher(
        similarity_threshold=0.5
    )

    matched, missing = matcher.match_requirements(
        mock_repo,
        [mock_requirement_with_embedding],
        resume_fingerprint="test-fingerprint"
    )

    assert len(matched) == 0
    assert len(missing) == 1
    assert missing[0].is_covered == False
    assert missing[0].similarity == 0.4
    assert missing[0].evidence is not None
    assert missing[0].evidence.id == "reu_001"
    assert missing[0].evidence.text == "Python development"
