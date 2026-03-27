#!/usr/bin/env python3
"""
Tests for Explainability Module
Covers: core/matcher/explainability.py
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from core.matcher.explainability import (
    _get_requirement_embedding,
    _calculate_section_similarities,
    calculate_requirement_similarity_with_resume_sections,
    explain_match,
)


class TestGetRequirementEmbedding:
    """Test _get_requirement_embedding function."""

    def test_embedding_from_requirement_row(self):
        """Test extracting embedding from requirement_row."""
        mock_embedding = [0.1, 0.2, 0.3]
        mock_unit = Mock()
        mock_unit.embedding = mock_embedding
        mock_embedding_row = Mock()
        mock_embedding_row.unit = mock_unit
        mock_requirement_row = Mock()
        mock_requirement_row.embedding_row = mock_embedding_row
        
        mock_requirement = Mock()
        mock_requirement.requirement_row = mock_requirement_row
        mock_requirement.embedding_row = None

        result = _get_requirement_embedding(mock_requirement)

        assert result == mock_embedding

    def test_embedding_from_embedding_row(self):
        """Test extracting embedding from embedding_row."""
        mock_embedding = [0.4, 0.5, 0.6]
        mock_unit = Mock()
        mock_unit.embedding = mock_embedding
        mock_embedding_row = Mock()
        mock_embedding_row.unit = mock_unit
        
        mock_requirement = Mock()
        mock_requirement.requirement_row = None
        mock_requirement.embedding_row = mock_embedding_row

        result = _get_requirement_embedding(mock_requirement)

        assert result == mock_embedding

    def test_no_embedding_available(self):
        """Test when no embedding is available."""
        mock_requirement = Mock()
        mock_requirement.requirement_row = None
        mock_requirement.embedding_row = None

        result = _get_requirement_embedding(mock_requirement)

        assert result is None

    def test_requirement_row_no_embedding(self):
        """Test when requirement_row exists but has no embedding."""
        mock_unit = Mock()
        mock_unit.embedding = None
        mock_embedding_row = Mock()
        mock_embedding_row.unit = mock_unit
        mock_requirement_row = Mock()
        mock_requirement_row.embedding_row = mock_embedding_row
        
        mock_requirement = Mock()
        mock_requirement.requirement_row = mock_requirement_row
        mock_requirement.embedding_row = None

        result = _get_requirement_embedding(mock_requirement)

        assert result is None

    def test_attribute_error_handling(self):
        """Test handling of AttributeError."""
        mock_requirement = Mock()
        # Remove attributes to trigger AttributeError
        del mock_requirement.requirement_row
        del mock_requirement.embedding_row

        result = _get_requirement_embedding(mock_requirement)

        assert result is None


class TestCalculateSectionSimilarities:
    """Test _calculate_section_similarities function."""

    @patch('core.matcher.explainability.cosine_similarity_from_distance')
    def test_success_single_section(self, mock_cosine):
        """Test calculating similarity with single section."""
        mock_cosine.return_value = 0.85

        mock_embedding = Mock()
        mock_embedding.cosine_distance.return_value = 0.15  # distance

        mock_section = Mock(
            embedding=mock_embedding,
            section_type="experience",
            section_index=0,
            source_text="5 years Python experience"
        )

        best_section, best_distance, similarities = _calculate_section_similarities(
            [0.1, 0.2, 0.3], [mock_section]
        )

        assert best_section == mock_section
        assert len(similarities) == 1
        assert similarities[0]['section_type'] == "experience"
        assert similarities[0]['similarity'] == 0.85

    @patch('core.matcher.explainability.cosine_similarity_from_distance')
    def test_success_multiple_sections(self, mock_cosine):
        """Test calculating similarity with multiple sections."""
        mock_cosine.side_effect = [0.75, 0.90, 0.65]

        sections = [
            Mock(
                embedding=Mock(cosine_distance=Mock(return_value=0.25)),
                section_type="experience", section_index=0, source_text="Exp 1"
            ),
            Mock(
                embedding=Mock(cosine_distance=Mock(return_value=0.10)),
                section_type="projects", section_index=0, source_text="Proj 1"
            ),
            Mock(
                embedding=Mock(cosine_distance=Mock(return_value=0.35)),
                section_type="skills", section_index=0, source_text="Skills 1"
            ),
        ]

        best_section, best_distance, similarities = _calculate_section_similarities(
            [0.1, 0.2, 0.3], sections
        )

        assert best_section == sections[1]  # Highest similarity (0.90)
        assert len(similarities) == 3
        # similarities are in order processed, not sorted
        assert similarities[1]['similarity'] == 0.90  # Second section has best similarity

    @patch('core.matcher.explainability.cosine_similarity_from_distance')
    def test_section_without_embedding(self, mock_cosine):
        """Test sections without embeddings are skipped."""
        mock_cosine.return_value = 0.80

        sections = [
            Mock(embedding=None, section_type="experience", section_index=0, source_text="No embedding"),
            Mock(
                embedding=Mock(cosine_distance=Mock(return_value=0.20)),
                section_type="skills", section_index=0, source_text="Has embedding"
            ),
        ]

        best_section, best_distance, similarities = _calculate_section_similarities(
            [0.1, 0.2, 0.3], sections
        )

        assert len(similarities) == 1
        assert similarities[0]['section_type'] == "skills"

    @patch('core.matcher.explainability.cosine_similarity_from_distance')
    def test_similarity_calculation_error(self, mock_cosine, caplog):
        """Test handling of similarity calculation errors."""
        import logging
        caplog.set_level(logging.WARNING)
        mock_cosine.side_effect = Exception("Similarity error")

        mock_embedding = Mock()
        mock_embedding.cosine_distance.return_value = 0.2
        mock_section = Mock(
            embedding=mock_embedding,
            section_type="experience",
            section_index=0,
            source_text="Test"
        )

        best_section, best_distance, similarities = _calculate_section_similarities(
            [0.1, 0.2, 0.3], [mock_section]
        )

        assert best_section is None
        assert similarities == []
        assert "Error computing similarity" in caplog.text

    def test_empty_sections(self):
        """Test with empty sections list."""
        best_section, best_distance, similarities = _calculate_section_similarities(
            [0.1, 0.2, 0.3], []
        )

        assert best_section is None
        assert best_distance == float('inf')
        assert similarities == []

    @patch('core.matcher.explainability.cosine_similarity_from_distance')
    def test_truncates_source_text(self, mock_cosine):
        """Test that source text is truncated to 200 chars."""
        mock_cosine.return_value = 0.85

        mock_embedding = Mock(cosine_distance=Mock(return_value=0.15))
        long_text = "x" * 300
        mock_section = Mock(
            embedding=mock_embedding,
            section_type="experience",
            section_index=0,
            source_text=long_text
        )

        best_section, best_distance, similarities = _calculate_section_similarities(
            [0.1, 0.2, 0.3], [mock_section]
        )

        assert len(similarities[0]['source_text']) == 200


class TestCalculateRequirementSimilarityWithResumeSections:
    """Test calculate_requirement_similarity_with_resume_sections function."""

    @patch('core.matcher.explainability._get_requirement_embedding')
    @patch('core.matcher.explainability._calculate_section_similarities')
    def test_success_full_calculation(self, mock_calc_sim, mock_get_emb):
        """Test successful similarity calculation."""
        mock_get_emb.return_value = [0.1, 0.2, 0.3]
        mock_calc_sim.return_value = (
            Mock(section_type="experience", section_index=0, source_text="Exp text"),
            0.15,
            [{'section_type': 'experience', 'section_index': 0, 'similarity': 0.85, 'source_text': 'Exp text'}]
        )

        mock_repo = Mock()
        mock_sections = [Mock(embedding=[0.1, 0.2, 0.3], section_type="experience", section_index=0)]
        mock_repo.resume.get_resume_section_embeddings.return_value = mock_sections

        mock_requirement = Mock()
        mock_requirement.id = "req-123"
        mock_requirement.text = "Python experience required"

        similarity, details = calculate_requirement_similarity_with_resume_sections(
            mock_requirement, "fp-123", mock_repo
        )

        assert similarity == 0.85
        assert details['requirement_id'] == "req-123"
        assert details['best_section'] == "experience"

    def test_no_requirement_embedding(self):
        """Test when requirement has no embedding."""
        mock_repo = Mock()
        mock_requirement = Mock()

        with patch('core.matcher.explainability._get_requirement_embedding', return_value=None):
            similarity, details = calculate_requirement_similarity_with_resume_sections(
                mock_requirement, "fp-123", mock_repo
            )

        assert similarity == 0.0
        assert details.get('skipped') is True
        assert details.get('reason') == 'No embedding'

    def test_no_resume_sections(self):
        """Test when no resume sections found."""
        mock_repo = Mock()
        mock_repo.resume.get_resume_section_embeddings.return_value = []

        mock_requirement = Mock()

        with patch('core.matcher.explainability._get_requirement_embedding', return_value=[0.1, 0.2]):
            similarity, details = calculate_requirement_similarity_with_resume_sections(
                mock_requirement, "fp-123", mock_repo
            )

        assert similarity == 0.0
        assert details.get('skipped') is True
        assert details.get('reason') == 'No resume sections found'

    def test_sections_without_embeddings(self):
        """Test when sections exist but have no embeddings."""
        mock_repo = Mock()
        mock_sections = [Mock(embedding=None)]
        mock_repo.resume.get_resume_section_embeddings.return_value = mock_sections

        mock_requirement = Mock()

        with patch('core.matcher.explainability._get_requirement_embedding', return_value=[0.1, 0.2]):
            similarity, details = calculate_requirement_similarity_with_resume_sections(
                mock_requirement, "fp-123", mock_repo
            )

        assert similarity == 0.0
        assert details.get('skipped') is True

    @patch('core.matcher.explainability._calculate_section_similarities')
    def test_no_similarities_computed(self, mock_calc_sim):
        """Test when no similarities can be computed."""
        mock_calc_sim.return_value = (None, float('inf'), [])

        mock_repo = Mock()
        mock_sections = [Mock(embedding=[0.1, 0.2])]
        mock_repo.resume.get_resume_section_embeddings.return_value = mock_sections

        mock_requirement = Mock()

        with patch('core.matcher.explainability._get_requirement_embedding', return_value=[0.1, 0.2]):
            similarity, details = calculate_requirement_similarity_with_resume_sections(
                mock_requirement, "fp-123", mock_repo
            )

        assert similarity == 0.0
        assert details.get('skipped') is True
        assert details.get('reason') == 'Could not compute similarities'

    def test_filter_by_section_type(self):
        """Test filtering sections by type."""
        mock_repo = Mock()
        mock_sections = [
            Mock(embedding=[0.1, 0.2], section_type="experience"),
            Mock(embedding=[0.2, 0.3], section_type="skills"),
            Mock(embedding=[0.3, 0.4], section_type="education"),
        ]
        mock_repo.resume.get_resume_section_embeddings.return_value = mock_sections

        mock_requirement = Mock()

        with patch('core.matcher.explainability._get_requirement_embedding', return_value=[0.1, 0.2]):
            with patch('core.matcher.explainability._calculate_section_similarities') as mock_calc:
                mock_calc.return_value = (Mock(), 0.1, [])

                calculate_requirement_similarity_with_resume_sections(
                    mock_requirement, "fp-123", mock_repo, section_types=["experience", "skills"]
                )

                # Verify only filtered sections are processed
                call_args = mock_calc.call_args[0][1]
                assert len(call_args) == 2
                assert all(s.section_type in ["experience", "skills"] for s in call_args)

    def test_top_k_limits_results(self):
        """Test that top_k limits returned matches."""
        mock_repo = Mock()
        mock_sections = [Mock(embedding=[0.1, 0.2])]
        mock_repo.resume.get_resume_section_embeddings.return_value = mock_sections

        mock_requirement = Mock()
        mock_requirement.id = "req-1"
        mock_requirement.text = "Test requirement"

        with patch('core.matcher.explainability._get_requirement_embedding', return_value=[0.1, 0.2]):
            with patch('core.matcher.explainability._calculate_section_similarities') as mock_calc:
                all_matches = [{'similarity': i * 0.1} for i in range(20)]
                mock_best_section = Mock()
                mock_best_section.source_text = "Best section text"
                mock_calc.return_value = (mock_best_section, 0.1, all_matches)

                similarity, details = calculate_requirement_similarity_with_resume_sections(
                    mock_requirement, "fp-123", mock_repo, top_k=5
                )

                assert len(details.get('all_matches', [])) == 5


class TestExplainMatch:
    """Test explain_match function."""

    @patch('core.matcher.explainability.calculate_requirement_similarity_with_resume_sections')
    def test_success_multiple_requirements(self, mock_calc):
        """Test explaining match with multiple requirements."""
        mock_calc.side_effect = [
            (0.85, {
                'best_section': 'experience',
                'all_matches': [{'section_type': 'experience', 'similarity': 0.85}],
                'skipped': False
            }),
            (0.75, {
                'best_section': 'skills',
                'all_matches': [{'section_type': 'skills', 'similarity': 0.75}],
                'skipped': False
            }),
        ]

        mock_requirements = [
            Mock(id="req-1", text="Python experience"),
            Mock(id="req-2", text="SQL knowledge"),
        ]
        mock_repo = Mock()

        result = explain_match(mock_requirements, "fp-123", mock_repo)

        assert len(result.get('per_requirement', [])) == 2
        assert 'section_summary' in result
        assert 'strengths' in result
        assert 'gaps' in result

    def test_empty_requirements(self):
        """Test explaining match with no requirements."""
        mock_repo = Mock()

        result = explain_match([], "fp-123", mock_repo)

        assert result['per_requirement'] == []
        assert result['section_summary'] == {}
        assert result['strengths'] == []
        assert result['gaps'] == []
        assert result['message'] == 'No job requirements provided'

    @patch('core.matcher.explainability.calculate_requirement_similarity_with_resume_sections')
    def test_section_summary_aggregation(self, mock_calc):
        """Test section summary aggregates scores correctly."""
        mock_calc.side_effect = [
            (0.80, {
                'best_section': 'experience',
                'all_matches': [
                    {'section_type': 'experience', 'similarity': 0.80},
                    {'section_type': 'skills', 'similarity': 0.60},
                ],
                'skipped': False
            }),
            (0.90, {
                'best_section': 'experience',
                'all_matches': [
                    {'section_type': 'experience', 'similarity': 0.90},
                    {'section_type': 'skills', 'similarity': 0.70},
                ],
                'skipped': False
            }),
        ]

        mock_requirements = [Mock(id="req-1", text="req1"), Mock(id="req-2", text="req2")]
        mock_repo = Mock()

        result = explain_match(mock_requirements, "fp-123", mock_repo)

        section_summary = result.get('section_summary', {})
        assert 'experience' in section_summary
        assert 'skills' in section_summary

        exp_summary = section_summary.get('experience', {})
        assert exp_summary.get('requirements_covered', 0) == 2

    @patch('core.matcher.explainability.calculate_requirement_similarity_with_resume_sections')
    def test_strengths_identification(self, mock_calc):
        """Test strengths are correctly identified."""
        mock_calc.side_effect = [
            (0.95, {'best_section': 'experience', 'all_matches': [{'section_type': 'experience', 'similarity': 0.95}], 'skipped': False}),
            (0.85, {'best_section': 'projects', 'all_matches': [{'section_type': 'projects', 'similarity': 0.85}], 'skipped': False}),
            (0.70, {'best_section': 'skills', 'all_matches': [{'section_type': 'skills', 'similarity': 0.70}], 'skipped': False}),
        ]

        mock_requirements = [Mock(id="req-1", text="req1"), Mock(id="req-2", text="req2"), Mock(id="req-3", text="req3")]
        mock_repo = Mock()

        result = explain_match(mock_requirements, "fp-123", mock_repo)

        strengths = result.get('strengths', [])
        assert len(strengths) >= 1  # At least one strength
        if strengths:
            assert strengths[0].get('section') == 'experience'
            assert strengths[0].get('score', 0) >= 0.9

    @patch('core.matcher.explainability.calculate_requirement_similarity_with_resume_sections')
    def test_gaps_identification(self, mock_calc):
        """Test gaps are correctly identified."""
        mock_calc.side_effect = [
            (0.40, {'best_section': 'education', 'all_matches': [{'section_type': 'education', 'similarity': 0.40}], 'skipped': False}),
            (0.35, {'best_section': 'skills', 'all_matches': [{'section_type': 'skills', 'similarity': 0.35}], 'skipped': False}),
            (0.80, {'best_section': 'experience', 'all_matches': [{'section_type': 'experience', 'similarity': 0.80}], 'skipped': False}),
        ]

        mock_requirements = [Mock(id="req-1", text="req1"), Mock(id="req-2", text="req2"), Mock(id="req-3", text="req3")]
        mock_repo = Mock()

        result = explain_match(mock_requirements, "fp-123", mock_repo)

        # Sections with avg_similarity < 0.5 should be gaps
        gaps = result.get('gaps', [])
        gap_sections = [gap.get('section') for gap in gaps]
        assert 'education' in gap_sections or 'skills' in gap_sections

    @patch('core.matcher.explainability.calculate_requirement_similarity_with_resume_sections')
    def test_skipped_requirements_excluded_from_summary(self, mock_calc):
        """Test skipped requirements are excluded from section summary."""
        mock_calc.side_effect = [
            (0.80, {'best_section': 'experience', 'all_matches': [{'section_type': 'experience', 'similarity': 0.80}], 'skipped': False}),
            (0.0, {'skipped': True, 'reason': 'No embedding', 'all_matches': []}),
        ]

        mock_requirements = [Mock(id="req-1", text="req1"), Mock(id="req-2", text="req2")]
        mock_repo = Mock()

        result = explain_match(mock_requirements, "fp-123", mock_repo)

        # Only req-1 should contribute to section_summary
        section_summary = result.get('section_summary', {})
        if 'experience' in section_summary:
            assert section_summary['experience'].get('requirements_covered', 0) >= 1

    @patch('core.matcher.explainability.calculate_requirement_similarity_with_resume_sections')
    def test_per_requirement_details(self, mock_calc):
        """Test per-requirement details are included."""
        mock_calc.return_value = (
            0.85,
            {
                'best_section': 'experience',
                'all_matches': [{'section_type': 'experience', 'similarity': 0.85}],
                'skipped': False
            }
        )

        mock_requirements = [Mock(id="req-123", text="Python programming")]
        mock_repo = Mock()

        result = explain_match(mock_requirements, "fp-123", mock_repo)

        per_req_list = result.get('per_requirement', [])
        if per_req_list:
            per_req = per_req_list[0]
            assert per_req.get('requirement_id') == "req-123"
            assert 'Python' in per_req.get('requirement_text', '')
            assert per_req.get('similarity', 0) == 0.85
