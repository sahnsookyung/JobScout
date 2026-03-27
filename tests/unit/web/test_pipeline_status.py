#!/usr/bin/env python3
"""Unit tests for pipeline status and event endpoint schema."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))


class TestPipelineStatusOpenAPI(unittest.TestCase):
    """Verify pipeline status endpoints document 404 responses."""

    def setUp(self):
        from fastapi import FastAPI
        from web.backend.routers.pipeline import router, limiter

        limiter.enabled = False
        self.app = FastAPI()
        self.app.include_router(router)

    def test_status_endpoint_documents_not_found_response(self):
        schema = self.app.openapi()
        responses = schema["paths"]["/api/pipeline/status/{task_id}"]["get"]["responses"]

        self.assertIn("404", responses)
        self.assertEqual(responses["404"]["description"], "Task not found")

    def test_events_endpoint_documents_not_found_response(self):
        schema = self.app.openapi()
        responses = schema["paths"]["/api/pipeline/events/{task_id}"]["get"]["responses"]

        self.assertIn("404", responses)
        self.assertEqual(responses["404"]["description"], "Task not found")
