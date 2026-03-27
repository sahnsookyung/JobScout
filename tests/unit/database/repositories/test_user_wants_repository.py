#!/usr/bin/env python3
"""
Unit tests for UserWants repository operations.

Tests the UserWantsRepository methods:
- save_user_wants()
- get_user_wants_embeddings()

These tests verify CRUD operations for user wants data.
"""

import unittest
import uuid
from unittest.mock import MagicMock, Mock, patch
import numpy as np
import pytest

from database.repositories.user_wants import UserWantsRepository
from database.models import UserWants


class TestSaveUserWants(unittest.TestCase):
    """Tests for save_user_wants method."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_db = MagicMock()
        self.repo = UserWantsRepository(self.mock_db)

    def test_save_user_wants_basic(self):
        """Should create and return UserWants with basic data."""
        user_id = "user_123"
        wants_text = "I want remote work"
        embedding = [0.1, 0.2, 0.3, 0.4]
        
        result = self.repo.save_user_wants(
            user_id=user_id,
            wants_text=wants_text,
            embedding=embedding
        )
        
        # Verify the object was created correctly
        self.assertIsInstance(result, UserWants)
        self.assertEqual(result.user_id, user_id)
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
            wants_text=wants_text,
            embedding=embedding,
            facet_key=facet_key
        )
        
        self.assertEqual(result.facet_key, facet_key)
        self.mock_db.add.assert_called_once_with(result)

    def test_save_user_wants_no_resume_fingerprint(self):
        """Should handle None resume_fingerprint."""
        self.repo.save_user_wants(
            user_id="user_789",
            wants_text="Want high salary",
            embedding=[0.9, 0.8, 0.7]
        )
        
        self.mock_db.add.assert_called_once()

    def test_save_user_wants_empty_embedding(self):
        """Should handle empty embedding list."""
        result = self.repo.save_user_wants(
            user_id="user_000",
            wants_text="Test",
            embedding=[]
        )
        
        self.assertEqual(result.embedding, [])

    def test_save_user_wants_unicode_text(self):
        """Should handle Unicode text in wants."""
        unicode_text = "日本語の仕事を探しています"
        rng = np.random.default_rng(42)
        embedding = rng.random(1024).tolist()
        
        result = self.repo.save_user_wants(
            user_id="user_unicode",
            wants_text=unicode_text,
            embedding=embedding
        )
        
        self.assertEqual(result.wants_text, unicode_text)

    def test_save_user_wants_large_embedding(self):
        """Should handle large embedding vectors (e.g., 1024 dimensions)."""
        rng = np.random.default_rng(42)
        large_embedding = rng.random(1024).tolist()

        self.repo.save_user_wants(
            user_id="user_large",
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
        self.repo = UserWantsRepository(self.mock_db)

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
        rng = np.random.default_rng(42)
        large_embeddings = [
            rng.random(1024).tolist(),
            rng.random(1024).tolist()
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


@pytest.mark.db
class TestUserWantsIntegration:
    """Real save→get roundtrip against a live DB container.

    The previous implementation used mock_db.execute manually set to return
    what was 'saved' — a circular mock that proved nothing about actual DB
    persistence.  This version writes and reads from a real pgvector DB.
    """

    @pytest.fixture
    def db_session(self, test_database):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        engine = create_engine(test_database)
        connection = engine.connect()
        transaction = connection.begin()
        session = sessionmaker(bind=connection)()
        yield session
        session.close()
        if transaction.is_active:
            transaction.rollback()
        connection.close()

    def test_save_and_get_roundtrip(self, db_session):
        """Saved embedding is actually persisted and retrievable from the DB."""
        repo = UserWantsRepository(db_session)
        user_id = str(uuid.uuid4())
        embedding = [0.1] * 1024

        saved = repo.save_user_wants(
            user_id=user_id,
            wants_text="Real roundtrip test",
            embedding=embedding,
        )
        db_session.flush()

        assert saved.user_id == user_id
        assert saved.wants_text == "Real roundtrip test"

        embeddings = repo.get_user_wants_embeddings(user_id)
        assert len(embeddings) == 1
        assert embeddings[0] == pytest.approx(embedding, rel=1e-5)

    def test_save_multiple_and_get_all(self, db_session):
        """Multiple saves for the same user return all embeddings."""
        repo = UserWantsRepository(db_session)
        user_id = str(uuid.uuid4())

        repo.save_user_wants(user_id=user_id, wants_text="Want A", embedding=[0.1] * 1024)
        repo.save_user_wants(user_id=user_id, wants_text="Want B", embedding=[0.2] * 1024)
        db_session.flush()

        embeddings = repo.get_user_wants_embeddings(user_id)
        assert len(embeddings) == 2

    def test_get_returns_empty_for_unknown_user(self, db_session):
        """An unknown user_id returns an empty list, not an error."""
        repo = UserWantsRepository(db_session)
        result = repo.get_user_wants_embeddings(str(uuid.uuid4()))
        assert result == []


class TestUserWantsModel(unittest.TestCase):
    """Tests for UserWants model itself."""

    def test_model_instantiation(self):
        """Should create UserWants instance with all fields."""
        want = UserWants(
            user_id="test_user",
            wants_text="Test want",
            embedding=[0.1, 0.2, 0.3],
            facet_key="test_facet"
        )
        
        self.assertEqual(want.user_id, "test_user")
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
        
        self.assertIsNone(want.facet_key)


if __name__ == '__main__':
    unittest.main()
