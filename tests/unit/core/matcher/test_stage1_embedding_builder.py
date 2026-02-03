#!/usr/bin/env python3
"""Test Stage1EmbeddingBuilder observability and weights clarity."""
import pytest
from unittest.mock import Mock

from core.matcher.stage1_embedding_builder import Stage1EmbeddingBuilder
from core.matcher.models import ResumeEvidenceUnit
from core.config_loader import Stage1EmbeddingConfig


@pytest.fixture
def sample_evidence_units():
    """Create sample evidence units with embeddings."""
    return [
        ResumeEvidenceUnit(
            id="reu_001",
            text="Python development at TechCorp",
            source_section="experience",
            tags={"type": "description"},
            embedding=[0.8, 0.2, 0.1, 0.3] * 256
        ),
        ResumeEvidenceUnit(
            id="reu_002",
            text="AWS cloud deployment",
            source_section="experience",
            tags={"type": "description"},
            embedding=[0.7, 0.3, 0.2, 0.4] * 256
        ),
        ResumeEvidenceUnit(
            id="reu_003",
            text="Python, Docker, Kubernetes skills",
            source_section="skills",
            tags={"type": "skill"},
            embedding=[0.9, 0.1, 0.0, 0.2] * 256
        )
    ]


@pytest.fixture
def pooled_config():
    """Create pooled_reu mode config."""
    return Stage1EmbeddingConfig(
        mode="pooled_reu",
        pooling_method="weighted_mean",
        section_weights={
            "summary": 3.0,
            "skills": 2.0,
            "experience": 1.5,
            "projects": 0.5,
            "education": 0.0
        }
    )


@pytest.fixture
def text_config():
    """Create text mode config."""
    return Stage1EmbeddingConfig(
        mode="text",
        text_evidence_slice_limit=5
    )


class TestStage1EmbeddingBuilderObservability:
    """Test suite for Stage1EmbeddingBuilder observability features."""
    
    def test_pooled_mode_details_contain_requested_and_actual(
        self, sample_evidence_units, pooled_config
    ):
        """Test that details contain requested_mode and actual_mode for pooled mode."""
        builder = Stage1EmbeddingBuilder(config=pooled_config)
        embedding, details = builder.build(sample_evidence_units)
        
        assert "requested_mode" in details
        assert details["requested_mode"] == "pooled_reu"
        assert "actual_mode" in details
        assert details["actual_mode"] == "pooled_reu"
        assert "fallback_reason" not in details
    
    def test_text_mode_without_ai_falls_back_to_pooled(
        self, sample_evidence_units, text_config
    ):
        """Test text mode without AI service falls back to pooled mode with reason."""
        builder = Stage1EmbeddingBuilder(config=text_config, ai_service=None)
        embedding, details = builder.build(sample_evidence_units)
        
        assert details["requested_mode"] == "text"
        assert details["actual_mode"] == "pooled_reu"
        assert "fallback_reason" in details
        assert details["fallback_reason"] == "ai_unavailable"
    
    def test_text_mode_with_ai_error_falls_back(
        self, sample_evidence_units, text_config
    ):
        """Test text mode with AI error falls back to pooled mode with reason."""
        mock_ai = Mock()
        mock_ai.generate_embedding.side_effect = Exception("AI service unavailable")
        
        builder = Stage1EmbeddingBuilder(config=text_config, ai_service=mock_ai)
        embedding, details = builder.build(sample_evidence_units)
        
        assert details["requested_mode"] == "text"
        assert details["actual_mode"] == "pooled_reu"
        assert "fallback_reason" in details
        assert details["fallback_reason"] == "ai_error"
    
    def test_text_mode_with_ai_succeeds(
        self, sample_evidence_units, text_config
    ):
        """Test text mode with working AI service succeeds."""
        mock_ai = Mock()
        mock_ai.generate_embedding.return_value = [0.1, 0.2, 0.3] * 341
        
        builder = Stage1EmbeddingBuilder(config=text_config, ai_service=mock_ai)
        embedding, details = builder.build(sample_evidence_units)
        
        assert details["requested_mode"] == "text"
        assert details["actual_mode"] == "text"
        assert "fallback_reason" not in details
        assert details["method"] == "text_embedding"


class TestStage1EmbeddingBuilderWeightsClarity:
    """Test suite for Stage1EmbeddingBuilder weights accounting clarity."""
    
    def test_weights_use_normalized_section_names(
        self, sample_evidence_units, pooled_config
    ):
        """Test that section_weights uses normalized section names."""
        builder = Stage1EmbeddingBuilder(config=pooled_config)
        embedding, details = builder.build(sample_evidence_units)
        
        assert "section_weights" in details
        # Weights should be keyed by normalized names (lowercase)
        assert "experience" in details["section_weights"]
        assert "skills" in details["section_weights"]
        # Not raw section names like "Experience" or "Skills"
        assert "Experience" not in details["section_weights"]
        assert "Skills" not in details["section_weights"]
    
    def test_raw_to_normalized_mapping_provided(
        self, sample_evidence_units, pooled_config
    ):
        """Test that raw_to_normalized_mapping is provided for clarity."""
        builder = Stage1EmbeddingBuilder(config=pooled_config)
        embedding, details = builder.build(sample_evidence_units)
        
        assert "raw_to_normalized_mapping" in details
        mapping = details["raw_to_normalized_mapping"]
        # Raw names should map to normalized names
        assert "experience" in mapping or "Experience" in mapping
        assert "skills" in mapping or "Skills" in mapping
    
    def test_weights_values_match_config(
        self, sample_evidence_units, pooled_config
    ):
        """Test that weight values in details match config values."""
        builder = Stage1EmbeddingBuilder(config=pooled_config)
        embedding, details = builder.build(sample_evidence_units)
        
        weights = details["section_weights"]
        # Check that weights are correct values from config
        assert weights.get("experience", 0) == pooled_config.section_weights.get("experience", 1.0)
        assert weights.get("skills", 0) == pooled_config.section_weights.get("skills", 1.0)
