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
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent.parent))


class TestResumeUploadEndpoint(unittest.TestCase):
    """Unit tests for resume upload endpoint."""

    def setUp(self):
        from fastapi.testclient import TestClient
        from web.backend.routers.pipeline import router
        from fastapi import FastAPI

        self.app = FastAPI()
        self.app.include_router(router)
        self.client = TestClient(self.app, raise_server_exceptions=False)

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
                mock_etl_service.process_resume.return_value = (True, "test_fingerprint_123456", sample_resume)
                mock_context.build.return_value = mock_ctx

                with patch('database.uow.job_uow') as mock_uow:
                    mock_uow.return_value.__enter__ = MagicMock(return_value=mock_repo)
                    mock_uow.return_value.__exit__ = MagicMock(return_value=False)

                    response = self.client.post('/api/pipeline/upload-resume', files=files)

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertIn('uploaded successfully', data['message'])

    def test_upload_invalid_json_rejected(self):
        """Test that invalid JSON files are rejected."""
        invalid_json = "{ name: 'test' }"
        files = {'file': ('resume.json', invalid_json, 'application/json')}

        response = self.client.post('/api/pipeline/upload-resume', files=files)

        self.assertEqual(response.status_code, 400)
        self.assertIn('Invalid JSON', response.json()['detail'])

    def test_upload_non_json_file_rejected(self):
        """Test that non-JSON files are rejected."""
        files = {'file': ('resume.txt', 'some text content', 'text/plain')}

        response = self.client.post('/api/pipeline/upload-resume', files=files)

        self.assertEqual(response.status_code, 400)
        self.assertIn('Only JSON files', response.json()['detail'])

    def test_upload_without_extension_rejected(self):
        """Test that files without .json extension are rejected."""
        files = {'file': ('resume', '{"name": "test"}', 'application/json')}

        response = self.client.post('/api/pipeline/upload-resume', files=files)

        self.assertEqual(response.status_code, 400)
        self.assertIn('Only JSON files', response.json()['detail'])

    def test_upload_saves_to_configured_path(self):
        """Test that file is saved to configured path."""
        sample_resume = {"name": "Test", "sections": []}

        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_resume_path = os.path.join(tmp_dir, "custom_resume.json")

            with patch('core.config_loader.load_config') as mock_config:
                mock_cfg = MagicMock()
                mock_cfg.etl = MagicMock()
                # Use new config structure: etl.resume.resume_file
                mock_cfg.etl.resume = MagicMock()
                mock_cfg.etl.resume.resume_file = temp_resume_path
                mock_config.return_value = mock_cfg

                with patch('core.app_context.AppContext') as mock_context:
                    mock_ctx = MagicMock()
                    mock_etl_service = MagicMock()
                    mock_ctx.job_etl_service = mock_etl_service
                    mock_repo = MagicMock()
                    mock_etl_service.process_resume.return_value = (True, "fp123456", sample_resume)
                    mock_context.build.return_value = mock_ctx

                    with patch('database.uow.job_uow') as mock_uow:
                        mock_uow.return_value.__enter__ = MagicMock(return_value=mock_repo)
                        mock_uow.return_value.__exit__ = MagicMock(return_value=False)

                        files = {'file': ('resume.json', json.dumps(sample_resume), 'application/json')}
                        response = self.client.post('/api/pipeline/upload-resume', files=files)

            self.assertEqual(response.status_code, 200)
            self.assertTrue(os.path.exists(temp_resume_path))

            with open(temp_resume_path) as f:
                saved_data = json.load(f)
            self.assertEqual(saved_data, sample_resume)

    def test_upload_overwrites_existing_file(self):
        """Test that uploading overwrites existing resume file."""
        original_resume = {"name": "Original", "sections": []}
        updated_resume = {"name": "Updated", "sections": []}

        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_resume_path = os.path.join(tmp_dir, "resume.json")

            with open(temp_resume_path, 'w') as f:
                json.dump(original_resume, f)

            with patch('core.config_loader.load_config') as mock_config:
                mock_cfg = MagicMock()
                mock_cfg.etl = MagicMock()
                # Use new config structure: etl.resume.resume_file
                mock_cfg.etl.resume = MagicMock()
                mock_cfg.etl.resume.resume_file = temp_resume_path
                mock_config.return_value = mock_cfg

                with patch('core.app_context.AppContext') as mock_context:
                    mock_ctx = MagicMock()
                    mock_etl_service = MagicMock()
                    mock_ctx.job_etl_service = mock_etl_service
                    mock_repo = MagicMock()
                    mock_etl_service.process_resume.return_value = (True, "fp_updated", updated_resume)
                    mock_context.build.return_value = mock_ctx

                    with patch('database.uow.job_uow') as mock_uow:
                        mock_uow.return_value.__enter__ = MagicMock(return_value=mock_repo)
                        mock_uow.return_value.__exit__ = MagicMock(return_value=False)

                        files = {'file': ('resume.json', json.dumps(updated_resume), 'application/json')}
                        response = self.client.post('/api/pipeline/upload-resume', files=files)

            self.assertEqual(response.status_code, 200)

            with open(temp_resume_path) as f:
                saved_data = json.load(f)
            self.assertEqual(saved_data['name'], "Updated")

    def test_upload_triggers_etl_processing(self):
        """Test that upload triggers ETL resume processing."""
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
                    mock_etl_service.process_resume.return_value = (True, "fingerprint_abc123", sample_resume)
                    mock_context.build.return_value = mock_ctx

                    with patch('database.uow.job_uow') as mock_uow:
                        mock_uow.return_value.__enter__ = MagicMock(return_value=mock_repo)
                        mock_uow.return_value.__exit__ = MagicMock(return_value=False)

                        files = {'file': ('resume.json', json.dumps(sample_resume), 'application/json')}
                        response = self.client.post('/api/pipeline/upload-resume', files=files)

            self.assertEqual(response.status_code, 200)
            mock_etl_service.process_resume.assert_called_once()

    def test_upload_continues_on_etl_error(self):
        """Test that upload succeeds even if ETL processing fails."""
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

                with patch('core.app_context.AppContext') as mock_context:
                    mock_context.build.side_effect = Exception("ETL Error")

                    files = {'file': ('resume.json', json.dumps(sample_resume), 'application/json')}
                    response = self.client.post('/api/pipeline/upload-resume', files=files)

            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.json()['success'])
            self.assertTrue(os.path.exists(temp_resume_path))

    def test_upload_with_empty_file_rejected(self):
        """Test that empty files are rejected."""
        files = {'file': ('resume.json', '', 'application/json')}

        response = self.client.post('/api/pipeline/upload-resume', files=files)

        self.assertEqual(response.status_code, 400)

    def test_upload_file_size_limit_exceeded(self):
        """Test that files exceeding size limit are rejected."""
        large_content = 'x' * (11 * 1024 * 1024)  # 11MB
        files = {'file': ('large_resume.json', large_content, 'application/json')}

        response = self.client.post('/api/pipeline/upload-resume', files=files)

        self.assertEqual(response.status_code, 400)
        self.assertIn('10MB', response.json()['detail'])

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
                    mock_etl_service.process_resume.return_value = (True, "test_fingerprint_1234567890", sample_resume)
                    mock_context.build.return_value = mock_ctx

                    with patch('database.uow.job_uow') as mock_uow:
                        mock_uow.return_value.__enter__ = MagicMock(return_value=mock_repo)
                        mock_uow.return_value.__exit__ = MagicMock(return_value=False)

                        files = {'file': ('resume.json', json.dumps(sample_resume), 'application/json')}
                        response = self.client.post('/api/pipeline/upload-resume', files=files)

            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertIn('fingerprint', data['message'])
            self.assertIn('test_fingerprint', data['message'])


if __name__ == '__main__':
    unittest.main()
