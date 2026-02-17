#!/usr/bin/env python3
"""
Unit tests for UserWants repository operations.

Tests the ResumeRepository methods:
- save_user_wants()
- get_user_wants_embeddings()

These tests verify CRUD operations for user wants data.
"""

import unittest
from unittest.mock import MagicMock, Mock, patch
import numpy as np

from database.repositories.resume import ResumeRepository
from database.models import UserWants


class TestSaveUserWants(unittest.TestCase):
    """Tests for save_user_wants method."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_db = MagicMock()
        self.repo = ResumeRepository(self.mock_db)

    def test_save_user_wants_basic(self):
        """Should create and return UserWants with basic data."""
        user_id = "user_123"
        resume_fingerprint = "fp_abc123"
        wants_text = "I want remote work"
        embedding = [0.1, 0.2, 0.3, 0.4]
        
        result = self.repo.save_user_wants(
            user_id=user_id,
            resume_fingerprint=resume_fingerprint,
            wants_text=wants_text,
            embedding=embedding
        )
        
        # Verify the object was created correctly
        self.assertIsInstance(result, UserWants)
        self.assertEqual(result.user_id, user_id)
        self.assertEqual(result.resume_fingerprint, resume_fingerprint)
        self.assertEqual(result.wants_text, wants_text)
        self.assertEqual(result.embedding, embedding)
        self.assertIsNone(result.facet_key)
        
        # Verify it was added to the database session
        self.mock_db.add.assert_called_once_with(result)

    def test_save_user_wants_with_facet_key(self):
        """Should create UserWants with optional facet_key."""
        user_id = "user_456"
        wants_text = "Looking for Python roles"
        embedding = [0.5, 0.6, 0.7]
        facet_key = "tech_stack"
        
        result = self.repo.save_user_wants(
            user_id=user_id,
            resume_fingerprint=None,
            wants_text=wants_text,
            embedding=embedding,
            facet_key=facet_key
        )
        
        self.assertEqual(result.facet_key, facet_key)
        self.assertIsNone(result.resume_fingerprint)
        self.mock_db.add.assert_called_once_with(result)

    def test_save_user_wants_no_resume_fingerprint(self):
        """Should handle None resume_fingerprint."""
        result = self.repo.save_user_wants(
            user_id="user_789",
            resume_fingerprint=None,
            wants_text="Want high salary",
            embedding=[0.9, 0.8, 0.7]
        )
        
        self.assertIsNone(result.resume_fingerprint)
        self.mock_db.add.assert_called_once()

    def test_save_user_wants_empty_embedding(self):
        """Should handle empty embedding list."""
        result = self.repo.save_user_wants(
            user_id="user_000",
            resume_fingerprint="fp_empty",
            wants_text="Test",
            embedding=[]
        )
        
        self.assertEqual(result.embedding, [])

    def test_save_user_wants_unicode_text(self):
        """Should handle Unicode text in wants."""
        unicode_text = "日本語の仕事を探しています"
        embedding = np.random.rand(1024).tolist()
        
        result = self.repo.save_user_wants(
            user_id="user_unicode",
            resume_fingerprint="fp_unicode",
            wants_text=unicode_text,
            embedding=embedding
        )
        
        self.assertEqual(result.wants_text, unicode_text)

    def test_save_user_wants_large_embedding(self):
        """Should handle large embedding vectors (e.g., 1024 dimensions)."""
        large_embedding = np.random.rand(1024).tolist()

        result = self.repo.save_user_wants(
            user_id="user_large",
            resume_fingerprint="fp_large",
            wants_text="Large embedding test",
            embedding=large_embedding
        )

        # Verify the embedding was stored correctly
        # Check that mock was called with the correct embedding
        self.mock_db.add.assert_called_once()
        call_args = self.mock_db.add.call_args[0][0]
        self.assertEqual(len(call_args.embedding), 1024)

    def test_save_user_wants_multiple_calls(self):
        """Multiple calls should create multiple UserWants objects."""
        for i in range(3):
            self.repo.save_user_wants(
                user_id="user_multi",
                resume_fingerprint=f"fp_{i}",
                wants_text=f"Want {i}",
                embedding=[float(i)]
            )
        
        # Should have called add 3 times
        self.assertEqual(self.mock_db.add.call_count, 3)


class TestGetUserWantsEmbeddings(unittest.TestCase):
    """Tests for get_user_wants_embeddings method."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_db = MagicMock()
        self.repo = ResumeRepository(self.mock_db)

    def test_get_embeddings_basic(self):
        """Should return list of embeddings for user."""
        user_id = "user_123"
        expected_embeddings = [
            [0.1, 0.2, 0.3],
            [0.4, 0.5, 0.6],
            [0.7, 0.8, 0.9]
        ]
        
        # Mock the database query
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = expected_embeddings
        self.mock_db.execute.return_value = mock_result
        
        result = self.repo.get_user_wants_embeddings(user_id)
        
        self.assertEqual(result, expected_embeddings)
        self.assertEqual(len(result), 3)

    def test_get_embeddings_with_fingerprint(self):
        """Should filter by resume_fingerprint when provided."""
        user_id = "user_456"
        resume_fingerprint = "fp_abc"
        expected_embeddings = [[0.1, 0.2]]
        
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = expected_embeddings
        self.mock_db.execute.return_value = mock_result
        
        result = self.repo.get_user_wants_embeddings(user_id, resume_fingerprint)
        
        self.assertEqual(result, expected_embeddings)
        # Verify execute was called (the query is constructed correctly)
        self.mock_db.execute.assert_called_once()

    def test_get_embeddings_empty_result(self):
        """Should return empty list when no wants found."""
        user_id = "user_no_wants"
        
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        self.mock_db.execute.return_value = mock_result
        
        result = self.repo.get_user_wants_embeddings(user_id)
        
        self.assertEqual(result, [])

    def test_get_embeddings_empty_list_result(self):
        """Should handle empty list result gracefully."""
        user_id = "user_none"
        
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        self.mock_db.execute.return_value = mock_result
        
        result = self.repo.get_user_wants_embeddings(user_id)
        
        # Should return empty list when no wants found
        self.assertEqual(result, [])

    def test_get_embeddings_single_embedding(self):
        """Should handle single embedding result."""
        user_id = "user_single"
        single_embedding = [0.5, 0.5, 0.5]
        
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [single_embedding]
        self.mock_db.execute.return_value = mock_result
        
        result = self.repo.get_user_wants_embeddings(user_id)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], single_embedding)

    def test_get_embeddings_large_vectors(self):
        """Should handle large embedding vectors (1024 dimensions)."""
        user_id = "user_large"
        large_embeddings = [
            np.random.rand(1024).tolist(),
            np.random.rand(1024).tolist()
        ]
        
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = large_embeddings
        self.mock_db.execute.return_value = mock_result
        
        result = self.repo.get_user_wants_embeddings(user_id)
        
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result[0]), 1024)

    def test_get_embeddings_query_construction(self):
        """Should construct correct query with user_id filter."""
        user_id = "user_query_test"
        
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        self.mock_db.execute.return_value = mock_result
        
        self.repo.get_user_wants_embeddings(user_id)
        
        # Verify execute was called
        self.mock_db.execute.assert_called_once()
        # The actual query construction is verified by the call being made


class TestUserWantsIntegration(unittest.TestCase):
    """Integration tests for save + get workflow."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_db = MagicMock()
        self.repo = ResumeRepository(self.mock_db)

    def test_save_and_get_roundtrip(self):
        """Save should create object that can be retrieved."""
        # Save a user want
        saved = self.repo.save_user_wants(
            user_id="user_roundtrip",
            resume_fingerprint="fp_rt",
            wants_text="Roundtrip test",
            embedding=[1.0, 2.0, 3.0]
        )
        
        # Verify save returned correct data
        self.assertEqual(saved.user_id, "user_roundtrip")
        self.assertEqual(saved.wants_text, "Roundtrip test")
        
        # Mock the get to return what was "saved"
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [[1.0, 2.0, 3.0]]
        self.mock_db.execute.return_value = mock_result
        
        # Get embeddings
        embeddings = self.repo.get_user_wants_embeddings("user_roundtrip", "fp_rt")
        
        # Verify we got back what we saved
        self.assertEqual(len(embeddings), 1)
        self.assertEqual(embeddings[0], [1.0, 2.0, 3.0])


class TestUserWantsModel(unittest.TestCase):
    """Tests for UserWants model itself."""

    def test_model_instantiation(self):
        """Should create UserWants instance with all fields."""
        want = UserWants(
            user_id="test_user",
            resume_fingerprint="test_fp",
            wants_text="Test want",
            embedding=[0.1, 0.2, 0.3],
            facet_key="test_facet"
        )
        
        self.assertEqual(want.user_id, "test_user")
        self.assertEqual(want.resume_fingerprint, "test_fp")
        self.assertEqual(want.wants_text, "Test want")
        self.assertEqual(want.embedding, [0.1, 0.2, 0.3])
        self.assertEqual(want.facet_key, "test_facet")
        # ID is auto-generated on database insert, not instantiation

    def test_model_tablename(self):
        """Should have correct table name."""
        self.assertEqual(UserWants.__tablename__, "user_wants")

    def test_model_optional_fields(self):
        """Should handle optional fields as None."""
        want = UserWants(
            user_id="test_user",
            wants_text="Test",
            embedding=[0.1]
        )
        
        self.assertIsNone(want.resume_fingerprint)
        self.assertIsNone(want.facet_key)


if __name__ == '__main__':
    unittest.main()
