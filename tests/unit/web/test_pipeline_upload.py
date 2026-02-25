#!/usr/bin/env python3
"""
Unit tests for pipeline upload resume endpoint.
Tests the POST /api/pipeline/upload-resume endpoint.
"""

import sys
import unittest
import json
import tempfile
import os
import hashlib
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent.parent))


class TestResumeUploadEndpoint(unittest.TestCase):
    """Unit tests for resume upload endpoint."""

    def setUp(self):
        from fastapi.testclient import TestClient
        from web.backend.routers.pipeline import router, limiter
        from fastapi import FastAPI

        # Disable rate limiting for tests
        limiter.enabled = False

        self.app = FastAPI()
        self.app.include_router(router)
        self.client = TestClient(self.app, raise_server_exceptions=False)

    def _create_upload_mocks(self, temp_resume_path: str, resume_exists: bool = False, process_result=None):
        """Helper to create common mocks for upload tests."""
        mock_config = patch('core.config_loader.load_config').start()
        cfg = MagicMock()
        cfg.etl = MagicMock()
        cfg.etl.resume_file = temp_resume_path
        mock_config.return_value = cfg

        mock_context = patch('core.app_context.AppContext').start()
        ctx = MagicMock()
        ctx.job_etl_service = MagicMock()
        repo = MagicMock()
        repo.resume.resume_hash_exists.return_value = resume_exists
        
        if process_result:
            ctx.job_etl_service.process_resume.return_value = process_result
        
        mock_context.build.return_value = ctx

        mock_uow = patch('database.uow.job_uow').start()
        mock_uow.return_value.__enter__ = MagicMock(return_value=repo)
        mock_uow.return_value.__exit__ = MagicMock(return_value=False)

        return {
            'config': mock_config,
            'context': mock_context,
            'uow': mock_uow,
            'repo': repo,
            'etl_service': ctx.job_etl_service
        }

    def _cleanup_mocks(self, mocks):
        """Helper to stop all mocks."""
        mocks['config'].stop()
        mocks['context'].stop()
        mocks['uow'].stop()

    def test_upload_valid_json_resume(self):
        """Test uploading a valid JSON resume file."""
        sample_resume = {
            "name": "Test User",
            "title": "Software Engineer",
            "sections": []
        }
        files = {'file': ('resume.json', json.dumps(sample_resume), 'application/json')}

        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_resume_path = os.path.join(tmp_dir, "test_resume.json")
            mocks = self._create_upload_mocks(
                temp_resume_path,
                resume_exists=False,
                process_result=(True, "test_fingerprint_123456", sample_resume)
            )

            response = self.client.post('/api/pipeline/upload-resume', files=files)
            self._cleanup_mocks(mocks)

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertIn('processed', data['message'].lower())

    def test_upload_unsupported_format_rejected(self):
        """Test that unsupported file formats are rejected."""
        files = {'file': ('resume.exe', 'binary content', 'application/octet-stream')}

        response = self.client.post('/api/pipeline/upload-resume', files=files)

        self.assertEqual(response.status_code, 400)
        self.assertIn('Unsupported file format', response.json()['detail'])

    def test_upload_without_extension_rejected(self):
        """Test that files without extension are rejected."""
        files = {'file': ('resume', '{"name": "test"}', 'application/json')}

        response = self.client.post('/api/pipeline/upload-resume', files=files)

        self.assertEqual(response.status_code, 400)
        self.assertIn('Unsupported file format', response.json()['detail'])

    def test_upload_txt_file_accepted(self):
        """Test that .txt files are now accepted (multi-format support)."""
        files = {'file': ('resume.txt', 'some text content', 'text/plain')}

        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_resume_path = os.path.join(tmp_dir, "resume.txt")
            mocks = self._create_upload_mocks(
                temp_resume_path,
                resume_exists=False,
                process_result=(True, "test_fingerprint_123", None)
            )

            response = self.client.post('/api/pipeline/upload-resume', files=files)
            self._cleanup_mocks(mocks)

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertIn('processed', data['message'].lower())

    def test_upload_triggers_etl_processing(self):
        """Test that upload triggers ETL resume processing."""
        sample_resume = {"name": "Test", "sections": []}

        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_resume_path = os.path.join(tmp_dir, "resume.json")
            mocks = self._create_upload_mocks(
                temp_resume_path,
                resume_exists=False,
                process_result=(True, "fingerprint_abc123", sample_resume)
            )

            response = self.client.post('/api/pipeline/upload-resume', files={'file': ('resume.json', json.dumps(sample_resume), 'application/json')})
            self._cleanup_mocks(mocks)

        self.assertEqual(response.status_code, 200)
        mocks['etl_service'].process_resume.assert_called_once()

    def test_upload_continues_on_etl_error(self):
        """Test that upload returns error response when ETL processing fails."""
        sample_resume = {"name": "Test", "sections": []}

        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_resume_path = os.path.join(tmp_dir, "resume.json")

            with patch('core.config_loader.load_config') as mock_config:
                mock_cfg = MagicMock()
                mock_cfg.etl = MagicMock()
                # Use new config structure: etl.resume.resume_file
                mock_cfg.etl.resume = MagicMock()
                mock_cfg.etl.resume.resume_file = temp_resume_path
                mock_config.return_value = mock_cfg

                # Mock database to return hash doesn't exist
                with patch('database.uow.job_uow') as mock_uow:
                    mock_repo = MagicMock()
                    mock_repo.resume.resume_hash_exists.return_value = False
                    mock_uow.return_value.__enter__ = MagicMock(return_value=mock_repo)
                    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

                    with patch('core.app_context.AppContext') as mock_context:
                        mock_context.build.side_effect = Exception("ETL Error")

                        files = {'file': ('resume.json', json.dumps(sample_resume), 'application/json')}
                        response = self.client.post('/api/pipeline/upload-resume', files=files)

            # Should return 200 with success=false when ETL fails
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertFalse(data['success'])
            self.assertIn('failed', data['message'].lower())

    def test_upload_with_empty_file_rejected(self):
        """Test that empty files are rejected."""
        files = {'file': ('resume.json', '', 'application/json')}

        response = self.client.post('/api/pipeline/upload-resume', files=files)

        self.assertEqual(response.status_code, 400)
        self.assertIn('Empty file', response.json()['detail'])

    def test_upload_file_size_limit_exceeded(self):
        """Test that files exceeding size limit are rejected."""
        large_content = 'x' * (3 * 1024 * 1024)  # 3MB > 2MB limit
        files = {'file': ('large_resume.json', large_content, 'application/json')}

        response = self.client.post('/api/pipeline/upload-resume', files=files)

        self.assertEqual(response.status_code, 400)
        self.assertIn('2MB', response.json()['detail'])

    def test_response_message_includes_fingerprint(self):
        """Test that successful response includes fingerprint in message."""
        sample_resume = {"name": "Test", "sections": []}

        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_resume_path = os.path.join(tmp_dir, "resume.json")

            with patch('core.config_loader.load_config') as mock_config:
                mock_cfg = MagicMock()
                mock_cfg.etl = MagicMock()
                mock_cfg.etl.resume_file = temp_resume_path
                mock_config.return_value = mock_cfg

                with patch('core.app_context.AppContext') as mock_context:
                    mock_ctx = MagicMock()
                    mock_etl_service = MagicMock()
                    mock_ctx.job_etl_service = mock_etl_service
                    mock_repo = MagicMock()
                    mock_repo.resume.resume_hash_exists.return_value = False
                    mock_etl_service.process_resume.return_value = (True, "test_fingerprint_1234567890", sample_resume)
                    mock_context.build.return_value = mock_ctx

                    with patch('database.uow.job_uow') as mock_uow:
                        mock_uow.return_value.__enter__ = MagicMock(return_value=mock_repo)
                        mock_uow.return_value.__exit__ = MagicMock(return_value=False)

                        files = {'file': ('resume.json', json.dumps(sample_resume), 'application/json')}
                        response = self.client.post('/api/pipeline/upload-resume', files=files)

            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertIn('resume_hash', data)


class TestResumeHashCheckEndpoint(unittest.TestCase):
    """Unit tests for resume hash check endpoint."""

    def setUp(self):
        from fastapi.testclient import TestClient
        from web.backend.routers.pipeline import router
        from fastapi import FastAPI

        self.app = FastAPI()
        self.app.include_router(router)
        self.client = TestClient(self.app, raise_server_exceptions=False)

    def test_check_resume_hash_exists_true(self):
        """Test /check-resume-hash returns exists=true when hash exists in DB."""
        test_hash = "abc123def45678901234567890123456"

        with patch('database.uow.job_uow') as mock_uow:
            mock_repo = MagicMock()
            mock_repo.resume.resume_hash_exists.return_value = True
            mock_uow.return_value.__enter__ = MagicMock(return_value=mock_repo)
            mock_uow.return_value.__exit__ = MagicMock(return_value=False)

            response = self.client.post(
                '/api/pipeline/check-resume-hash',
                json={'resume_hash': test_hash}
            )

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['exists'])
        self.assertEqual(data['resume_hash'], test_hash)

    def test_check_resume_hash_exists_false(self):
        """Test /check-resume-hash returns exists=false when hash not in DB."""
        test_hash = "nonexistent_hash_123456789"

        with patch('database.uow.job_uow') as mock_uow:
            mock_repo = MagicMock()
            mock_repo.resume.resume_hash_exists.return_value = False
            mock_uow.return_value.__enter__ = MagicMock(return_value=mock_repo)
            mock_uow.return_value.__exit__ = MagicMock(return_value=False)

            response = self.client.post(
                '/api/pipeline/check-resume-hash',
                json={'resume_hash': test_hash}
            )

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertFalse(data['exists'])
        self.assertEqual(data['resume_hash'], test_hash)

    def test_check_resume_hash_requires_hash(self):
        """Test /check-resume-hash rejects request without hash."""
        response = self.client.post(
            '/api/pipeline/check-resume-hash',
            json={}
        )

        self.assertEqual(response.status_code, 422)  # Validation error


class TestResumeUploadSecurity(unittest.TestCase):
    """Security tests for resume upload endpoint."""

    def setUp(self):
        from fastapi.testclient import TestClient
        from web.backend.routers.pipeline import router, limiter
        from fastapi import FastAPI

        # Disable rate limiting for tests
        limiter.enabled = False

        self.app = FastAPI()
        self.app.include_router(router)
        self.client = TestClient(self.app, raise_server_exceptions=False)

    def test_upload_file_size_limit_2mb(self):
        """Test that files exceeding 2MB limit are rejected."""
        large_content = 'x' * (3 * 1024 * 1024)  # 3MB > 2MB limit
        files = {'file': ('large_resume.json', large_content, 'application/json')}

        response = self.client.post('/api/pipeline/upload-resume', files=files)

        self.assertEqual(response.status_code, 400)
        self.assertIn('2MB', response.json()['detail'])

    def test_upload_hash_mismatch_rejected(self):
        """Test that server rejects file when client hash doesn't match computed hash (security)."""
        sample_content = b'{"name": "Test User"}'
        correct_hash = hashlib.sha256(sample_content).hexdigest()[:32]
        wrong_hash = "wrong_hash_that_attacker_provides"

        files = {'file': ('resume.json', sample_content, 'application/json')}

        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_resume_path = os.path.join(tmp_dir, "resume.json")

            with patch('core.config_loader.load_config') as mock_config:
                mock_cfg = MagicMock()
                mock_cfg.etl = MagicMock()
                mock_cfg.etl.resume = MagicMock()
                mock_cfg.etl.resume.resume_file = temp_resume_path
                mock_config.return_value = mock_cfg

                # Mock the database to avoid connection errors
                with patch('database.uow.job_uow') as mock_uow:
                    mock_repo = MagicMock()
                    mock_repo.resume.resume_hash_exists.return_value = False
                    mock_uow.return_value.__enter__ = MagicMock(return_value=mock_repo)
                    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

                    # Provide WRONG hash - server should detect mismatch
                    response = self.client.post(
                        '/api/pipeline/upload-resume',
                        files=files,
                        data={'resume_hash': wrong_hash}
                    )

        # Server should reject due to hash mismatch
        self.assertEqual(response.status_code, 400)
        self.assertIn('hash', response.json()['detail'].lower())

    def test_upload_hash_match_succeeds(self):
        """Test that server accepts when client hash matches computed hash."""
        sample_content = b'{"name": "Test User", "title": "Engineer"}'
        correct_hash = hashlib.sha256(sample_content).hexdigest()[:32]

        files = {'file': ('resume.json', sample_content, 'application/json')}

        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_resume_path = os.path.join(tmp_dir, "resume.json")

            with patch('core.config_loader.load_config') as mock_config:
                mock_cfg = MagicMock()
                mock_cfg.etl = MagicMock()
                mock_cfg.etl.resume = MagicMock()
                mock_cfg.etl.resume.resume_file = temp_resume_path
                mock_config.return_value = mock_cfg

                with patch('database.uow.job_uow') as mock_uow:
                    mock_repo = MagicMock()
                    # First call: hash doesn't exist (resume_hash_exists)
                    # Second call: save (process_resume)
                    mock_repo.resume.resume_hash_exists.return_value = False
                    mock_uow.return_value.__enter__ = MagicMock(return_value=mock_repo)
                    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

                    with patch('core.app_context.AppContext') as mock_context:
                        mock_ctx = MagicMock()
                        mock_etl_service = MagicMock()
                        mock_ctx.job_etl_service = mock_etl_service
                        mock_etl_service.process_resume.return_value = (True, correct_hash, {"raw_text": "test"})
                        mock_context.build.return_value = mock_ctx

                        response = self.client.post(
                            '/api/pipeline/upload-resume',
                            files=files,
                            data={'resume_hash': correct_hash}
                        )

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertEqual(data['resume_hash'], correct_hash)


class TestResumeUploadDeduplication(unittest.TestCase):
    """Tests for resume deduplication based on file hash."""

    def setUp(self):
        from fastapi.testclient import TestClient
        from web.backend.routers.pipeline import router, limiter
        from fastapi import FastAPI

        # Disable rate limiting for tests
        limiter.enabled = False

        self.app = FastAPI()
        self.app.include_router(router)
        self.client = TestClient(self.app, raise_server_exceptions=False)

    def test_upload_deduplication_skips_processing(self):
        """Test that uploading same file twice skips processing on second upload."""
        sample_content = b'{"name": "Test User", "experience": [{"title": "Engineer"}]}'
        file_hash = hashlib.sha256(sample_content).hexdigest()[:32]

        files = {'file': ('resume.json', sample_content, 'application/json')}

        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_resume_path = os.path.join(tmp_dir, "resume.json")

            with patch('core.config_loader.load_config') as mock_config:
                mock_cfg = MagicMock()
                mock_cfg.etl = MagicMock()
                mock_cfg.etl.resume = MagicMock()
                mock_cfg.etl.resume.resume_file = temp_resume_path
                mock_config.return_value = mock_cfg

                with patch('database.uow.job_uow') as mock_uow:
                    mock_repo = MagicMock()
                    # Simulate hash already exists in DB
                    mock_repo.resume.resume_hash_exists.return_value = True
                    mock_uow.return_value.__enter__ = MagicMock(return_value=mock_repo)
                    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

                    with patch('core.app_context.AppContext') as mock_context:
                        mock_ctx = MagicMock()
                        mock_etl_service = MagicMock()
                        mock_ctx.job_etl_service = mock_etl_service
                        mock_context.build.return_value = mock_ctx

                        response = self.client.post(
                            '/api/pipeline/upload-resume',
                            files=files,
                            data={'resume_hash': file_hash}
                        )

        # Should succeed without calling process_resume
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertIn('already processed', data['message'].lower())

    def test_upload_returns_hash_for_indexeddb(self):
        """Test that response includes resume_hash for frontend IndexedDB storage."""
        sample_content = b'{"name": "Test User"}'
        expected_hash = hashlib.sha256(sample_content).hexdigest()[:32]

        files = {'file': ('resume.json', sample_content, 'application/json')}

        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_resume_path = os.path.join(tmp_dir, "resume.json")

            with patch('core.config_loader.load_config') as mock_config:
                mock_cfg = MagicMock()
                mock_cfg.etl = MagicMock()
                mock_cfg.etl.resume = MagicMock()
                mock_cfg.etl.resume.resume_file = temp_resume_path
                mock_config.return_value = mock_cfg

                with patch('database.uow.job_uow') as mock_uow:
                    mock_repo = MagicMock()
                    mock_repo.resume.resume_hash_exists.return_value = False
                    mock_uow.return_value.__enter__ = MagicMock(return_value=mock_repo)
                    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

                    with patch('core.app_context.AppContext') as mock_context:
                        mock_ctx = MagicMock()
                        mock_etl_service = MagicMock()
                        mock_ctx.job_etl_service = mock_etl_service
                        mock_etl_service.process_resume.return_value = (True, expected_hash, {"raw_text": "test"})
                        mock_context.build.return_value = mock_ctx

                        response = self.client.post(
                            '/api/pipeline/upload-resume',
                            files=files,
                            data={'resume_hash': expected_hash}
                        )

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('resume_hash', data)
        self.assertEqual(data['resume_hash'], expected_hash)

    def test_upload_processes_new_file(self):
        """Test that new file gets processed and stored in DB."""
        sample_content = b'{"name": "New User", "title": "Developer"}'
        file_hash = hashlib.sha256(sample_content).hexdigest()[:32]

        files = {'file': ('resume.json', sample_content, 'application/json')}

        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_resume_path = os.path.join(tmp_dir, "resume.json")

            with patch('core.config_loader.load_config') as mock_config:
                mock_cfg = MagicMock()
                mock_cfg.etl = MagicMock()
                mock_cfg.etl.resume = MagicMock()
                mock_cfg.etl.resume.resume_file = temp_resume_path
                mock_config.return_value = mock_cfg

                with patch('database.uow.job_uow') as mock_uow:
                    mock_repo = MagicMock()
                    # First call: hash doesn't exist
                    mock_repo.resume.resume_hash_exists.return_value = False
                    mock_uow.return_value.__enter__ = MagicMock(return_value=mock_repo)
                    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

                    with patch('core.app_context.AppContext') as mock_context:
                        mock_ctx = MagicMock()
                        mock_etl_service = MagicMock()
                        mock_ctx.job_etl_service = mock_etl_service
                        mock_etl_service.process_resume.return_value = (True, file_hash, {"raw_text": "test"})
                        mock_context.build.return_value = mock_ctx

                        response = self.client.post(
                            '/api/pipeline/upload-resume',
                            files=files,
                            data={'resume_hash': file_hash}
                        )

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertIn('successfully', data['message'].lower())


if __name__ == '__main__':
    unittest.main()
