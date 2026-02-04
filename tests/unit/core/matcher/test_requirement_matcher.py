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
    return row


def test_requirement_matcher_covered(sample_evidence_units, mock_requirement_with_embedding, mock_repo, mock_evidence_row):
    """Test RequirementMatcher matches evidence to requirements using pgvector."""
    mock_repo.find_best_evidence_for_requirement.return_value = [(mock_evidence_row, 0.2)]

    matcher = RequirementMatcher(
        repo=mock_repo,
        similarity_threshold=0.5
    )

    matched, missing = matcher.match_requirements(
        sample_evidence_units,
        [mock_requirement_with_embedding],
        resume_fingerprint="test-fingerprint"
    )

    assert len(matched) == 1
    assert len(missing) == 0
    assert matched[0].is_covered == True
    assert matched[0].similarity >= 0.5


def test_requirement_matcher_not_covered(mock_requirement_no_embedding, mock_repo):
    """Test RequirementMatcher when requirement is not covered."""
    mock_repo.find_best_evidence_for_requirement.return_value = []

    matcher = RequirementMatcher(
        repo=mock_repo,
        similarity_threshold=0.6
    )

    evidence_units = [
        ResumeEvidenceUnit(
            id="reu_dissimilar",
            text="Completely unrelated skill",
            source_section="skills",
            tags={"type": "skill"},
            embedding=[0.0, 1.0, 0.0] * 341
        )
    ]

    matched, missing = matcher.match_requirements(
        evidence_units,
        [mock_requirement_no_embedding],
        resume_fingerprint="test-fingerprint"
    )

    assert len(matched) == 0
    assert len(missing) == 1
    assert missing[0].is_covered == False
    assert missing[0].similarity == 0.0


def test_requirement_matcher_threshold(mock_requirement_with_embedding, mock_repo, mock_evidence_row):
    """Test similarity threshold is respected."""
    mock_repo.find_best_evidence_for_requirement.return_value = [(mock_evidence_row, 0.4)]

    matcher = RequirementMatcher(
        repo=mock_repo,
        similarity_threshold=0.8
    )

    evidence_units = [
        ResumeEvidenceUnit(
            id="reu_moderate",
            text="Python development",
            source_section="skills",
            tags={"type": "skill"},
            embedding=[0.8, 0.2, 0.1] * 341
        )
    ]

    matched, missing = matcher.match_requirements(
        evidence_units,
        [mock_requirement_with_embedding],
        resume_fingerprint="test-fingerprint"
    )

    assert len(matched) == 0
    assert len(missing) == 1
    assert missing[0].is_covered == False
    assert missing[0].similarity < 0.8


def test_requirement_embedding_exception(mock_requirement_no_embedding, mock_repo):
    """Test that requirement embedding failure creates missing requirement."""
    mock_repo.find_best_evidence_for_requirement.return_value = []

    evidence_units = [
        ResumeEvidenceUnit(
            id="reu_001",
            text="Python development",
            source_section="skills",
            tags={"type": "skill"},
            embedding=[0.8, 0.2, 0.1] * 341
        )
    ]

    matcher = RequirementMatcher(
        repo=mock_repo,
        similarity_threshold=0.5
    )

    matched, missing = matcher.match_requirements(
        evidence_units,
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
    mock_repo.find_best_evidence_for_requirement.return_value = [(mock_evidence_row, 0.3)]

    matcher = RequirementMatcher(
        repo=mock_repo,
        similarity_threshold=0.5
    )

    evidence_units = [
        ResumeEvidenceUnit(
            id="reu_001",
            text="Python development",
            source_section="skills",
            tags={"type": "skill"},
            embedding=[0.8, 0.2, 0.1] * 341
        )
    ]

    matcher.match_requirements(
        evidence_units,
        [mock_requirement_with_embedding],
        resume_fingerprint="test-fingerprint-123"
    )

    mock_repo.find_best_evidence_for_requirement.assert_called_once()
    call_args = mock_repo.find_best_evidence_for_requirement.call_args
    assert call_args[1]['resume_fingerprint'] == "test-fingerprint-123"
    assert call_args[1]['top_k'] == 1
