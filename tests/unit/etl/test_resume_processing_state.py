#!/usr/bin/env python3
"""
Unit tests for resume processing state transitions and recovery behavior.
"""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
import shutil
from types import SimpleNamespace

from core.llm.openai_service import OpenAIService
from database.models import (
    RESUME_PROCESSING_EMBEDDING,
    RESUME_PROCESSING_EXTRACTED,
    RESUME_PROCESSING_EXTRACTING,
    RESUME_PROCESSING_FAILED,
    RESUME_PROCESSING_READY,
)
from database.repository import JobRepository
from etl.orchestrator import JobETLService


class TestResumeProcessingState(unittest.TestCase):
    """Tests resumable resume ETL behavior."""

    def setUp(self):
        self.mock_repo = MagicMock(spec=JobRepository)
        self.mock_ai = MagicMock(spec=OpenAIService)
        self.service = JobETLService(ai_service=self.mock_ai)

    def _make_resume_file(self, suffix: str = ".json") -> str:
        temp_dir = tempfile.mkdtemp()
        path = Path(temp_dir) / f"resume{suffix}"
        path.write_text('{"name": "Test User"}', encoding="utf-8")
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))
        return str(path)

    @patch("etl.orchestrator.generate_file_fingerprint", return_value="fp-ready")
    def test_process_resume_skips_when_fingerprint_is_ready(self, _mock_hash):
        resume_path = self._make_resume_file()
        self.mock_repo.is_resume_ready.return_value = True

        changed, fingerprint, resume_data = self.service.process_resume(self.mock_repo, resume_path)

        self.assertFalse(changed)
        self.assertEqual(fingerprint, "fp-ready")
        self.assertIsNone(resume_data)

    @patch("etl.orchestrator.ResumeParser")
    @patch("etl.orchestrator.generate_file_fingerprint", return_value="fp-ready")
    @patch.object(JobETLService, "extract_resume_one")
    def test_process_resume_force_re_extraction_overrides_ready_state(
        self,
        mock_extract,
        _mock_hash,
        mock_parser_cls,
    ):
        resume_path = self._make_resume_file()
        self.mock_repo.is_resume_ready.return_value = True
        self.mock_repo.get_resume_processing_state.return_value = None
        mock_parser_cls.return_value.parse.return_value = SimpleNamespace(
            data={"name": "Test User"},
            text='{"name": "Test User"}',
        )

        changed, fingerprint, resume_data = self.service.process_resume(
            self.mock_repo,
            resume_path,
            force_re_extraction=True,
        )

        self.assertTrue(changed)
        self.assertEqual(fingerprint, "fp-ready")
        self.assertEqual(resume_data, {"name": "Test User"})
        self.mock_repo.set_resume_processing_state.assert_called_once_with(
            "fp-ready",
            RESUME_PROCESSING_EXTRACTING,
            error=None,
        )
        mock_extract.assert_called_once_with(self.mock_repo, {"name": "Test User"}, "fp-ready")

    @patch("etl.orchestrator.generate_file_fingerprint", return_value="fp-extracted")
    @patch.object(JobETLService, "embed_resume_one")
    def test_process_resume_resumes_embedding_for_extracted_state(self, mock_embed, _mock_hash):
        resume_path = self._make_resume_file()
        self.mock_repo.is_resume_ready.return_value = False
        state = MagicMock()
        state.processing_status = RESUME_PROCESSING_EXTRACTED
        self.mock_repo.get_resume_processing_state.return_value = state

        changed, fingerprint, resume_data = self.service.process_resume(self.mock_repo, resume_path)

        self.assertTrue(changed)
        self.assertEqual(fingerprint, "fp-extracted")
        self.assertEqual(resume_data, {"name": "Test User"})
        mock_embed.assert_called_once_with(self.mock_repo, "fp-extracted")

    @patch("etl.orchestrator.generate_file_fingerprint", return_value="fp-extracted")
    @patch.object(JobETLService, "embed_resume_one", side_effect=RuntimeError("embed failed"))
    def test_process_resume_reraises_failed_resume_embedding_resume(self, _mock_embed, _mock_hash):
        resume_path = self._make_resume_file()
        self.mock_repo.is_resume_ready.return_value = False
        self.mock_repo.get_resume_processing_state.return_value = SimpleNamespace(
            processing_status=RESUME_PROCESSING_EXTRACTED
        )

        with self.assertRaisesRegex(RuntimeError, "embed failed"):
            self.service.process_resume(self.mock_repo, resume_path)

        self.assertEqual(
            self.mock_repo.set_resume_processing_state.call_args.args[:2],
            ("fp-extracted", RESUME_PROCESSING_FAILED),
        )

    @patch("etl.orchestrator.ResumeParser")
    @patch("etl.orchestrator.generate_file_fingerprint", return_value="fp-processing")
    def test_process_resume_skips_when_resume_is_already_processing(self, _mock_hash, mock_parser_cls):
        resume_path = self._make_resume_file()
        self.mock_repo.is_resume_ready.return_value = False
        self.mock_repo.get_resume_processing_state.return_value = SimpleNamespace(
            processing_status=RESUME_PROCESSING_EXTRACTING
        )

        changed, fingerprint, resume_data = self.service.process_resume(self.mock_repo, resume_path)

        self.assertFalse(changed)
        self.assertEqual(fingerprint, "fp-processing")
        self.assertIsNone(resume_data)
        mock_parser_cls.assert_not_called()

    @patch("etl.orchestrator.ResumeParser")
    @patch("etl.orchestrator.generate_file_fingerprint", return_value="fp-bad-parse")
    def test_process_resume_marks_failed_when_parser_raises(self, _mock_hash, mock_parser_cls):
        resume_path = self._make_resume_file()
        self.mock_repo.is_resume_ready.return_value = False
        self.mock_repo.get_resume_processing_state.return_value = None
        mock_parser_cls.return_value.parse.side_effect = ValueError("bad resume")

        changed, fingerprint, resume_data = self.service.process_resume(self.mock_repo, resume_path)

        self.assertFalse(changed)
        self.assertEqual(fingerprint, "")
        self.assertIsNone(resume_data)
        self.mock_repo.set_resume_processing_state.assert_called_once_with(
            "fp-bad-parse",
            RESUME_PROCESSING_FAILED,
            error="bad resume",
        )

    @patch("etl.orchestrator.ResumeParser")
    @patch("etl.orchestrator.generate_file_fingerprint", return_value="fp-new")
    @patch.object(JobETLService, "extract_resume_one")
    def test_process_resume_marks_extracting_before_full_extract(
        self,
        mock_extract,
        _mock_hash,
        mock_parser_cls,
    ):
        resume_path = self._make_resume_file()
        self.mock_repo.is_resume_ready.return_value = False
        self.mock_repo.get_resume_processing_state.return_value = None
        mock_parser_cls.return_value.parse.return_value = SimpleNamespace(
            data={"name": "Test User"},
            text='{"name": "Test User"}',
        )

        changed, fingerprint, resume_data = self.service.process_resume(self.mock_repo, resume_path)

        self.assertTrue(changed)
        self.assertEqual(fingerprint, "fp-new")
        self.assertEqual(resume_data, {"name": "Test User"})
        self.mock_repo.set_resume_processing_state.assert_called_once_with(
            "fp-new",
            RESUME_PROCESSING_EXTRACTING,
            error=None,
        )
        mock_extract.assert_called_once_with(self.mock_repo, {"name": "Test User"}, "fp-new")

    @patch("etl.orchestrator.ResumeParser")
    @patch("etl.orchestrator.generate_file_fingerprint", return_value="fp-fail")
    @patch.object(JobETLService, "extract_resume_one", side_effect=RuntimeError("extract failed"))
    def test_process_resume_marks_failed_when_full_extract_raises(
        self,
        _mock_extract,
        _mock_hash,
        mock_parser_cls,
    ):
        resume_path = self._make_resume_file()
        self.mock_repo.is_resume_ready.return_value = False
        self.mock_repo.get_resume_processing_state.return_value = None
        mock_parser_cls.return_value.parse.return_value = SimpleNamespace(
            data={"name": "Test User"},
            text='{"name": "Test User"}',
        )

        with self.assertRaisesRegex(RuntimeError, "extract failed"):
            self.service.process_resume(self.mock_repo, resume_path)

        self.assertEqual(
            self.mock_repo.set_resume_processing_state.call_args_list[0].args[:2],
            ("fp-fail", RESUME_PROCESSING_EXTRACTING),
        )
        self.assertEqual(
            self.mock_repo.set_resume_processing_state.call_args_list[-1].args[:2],
            ("fp-fail", RESUME_PROCESSING_FAILED),
        )

    @patch("etl.orchestrator.ResumeProfiler")
    @patch.object(JobETLService, "embed_resume_one")
    def test_extract_resume_one_saves_state_then_embeds(self, mock_embed, mock_profiler_cls):
        resume = MagicMock()
        resume.claimed_total_years = 7.0
        resume.model_dump.return_value = {"profile": {"summary": {"text": "Summary"}}}
        resume.extraction = SimpleNamespace(confidence=0.91, warnings=["minor"])
        mock_profiler_cls.return_value.extract_structured_resume.return_value = resume

        self.service.extract_resume_one(self.mock_repo, {"raw_text": "resume"}, "fp-structured")

        self.mock_repo.save_structured_resume.assert_called_once_with(
            resume_fingerprint="fp-structured",
            extracted_data=resume.model_dump.return_value,
            total_experience_years=7.0,
            extraction_confidence=0.91,
            extraction_warnings=["minor"],
        )
        self.mock_repo.set_resume_processing_state.assert_called_once()
        args, kwargs = self.mock_repo.set_resume_processing_state.call_args
        self.assertEqual(args[0], "fp-structured")
        self.assertEqual(args[1], RESUME_PROCESSING_EXTRACTED)
        self.assertIsNone(kwargs["error"])
        self.assertIn("extraction_completed_at", kwargs)
        mock_embed.assert_called_once_with(self.mock_repo, "fp-structured", resume)

    @patch("etl.orchestrator.ResumeProfiler")
    def test_extract_resume_one_raises_when_structured_resume_is_missing(self, mock_profiler_cls):
        mock_profiler_cls.return_value.extract_structured_resume.return_value = None

        with self.assertRaisesRegex(ValueError, "Structured resume extraction failed"):
            self.service.extract_resume_one(self.mock_repo, {"raw_text": "resume"}, "fp-missing")

    @patch("etl.orchestrator.JobRepositoryAdapter")
    @patch("etl.orchestrator.ResumeProfiler")
    @patch("etl.orchestrator.ResumeSchema.model_validate")
    def test_embed_resume_one_marks_resume_ready(
        self,
        mock_validate,
        mock_profiler_cls,
        mock_adapter_cls,
    ):
        structured = MagicMock()
        structured.extracted_data = {"profile": {"summary": {"text": "Summary"}}}
        self.mock_repo.resume.get_structured_resume_by_fingerprint.return_value = structured
        mock_validate.return_value = MagicMock()
        mock_profiler_cls.return_value.profile_resume.return_value = (
            MagicMock(),
            [MagicMock()],
            [{"section_type": "summary"}],
        )
        self.mock_repo.get_resume_summary_embedding.return_value = [0.1, 0.2]

        self.service.embed_resume_one(self.mock_repo, "fp-ready")

        first_call = self.mock_repo.set_resume_processing_state.call_args_list[0]
        self.assertEqual(first_call.args[0], "fp-ready")
        self.assertEqual(first_call.args[1], RESUME_PROCESSING_EMBEDDING)
        final_call = self.mock_repo.set_resume_processing_state.call_args_list[-1]
        self.assertEqual(final_call.args[0], "fp-ready")
        self.assertEqual(final_call.args[1], RESUME_PROCESSING_READY)
        self.assertIn("embedding_completed_at", final_call.kwargs)
        mock_adapter_cls.assert_called_once_with(self.mock_repo)

    def test_embed_resume_one_requires_structured_resume(self):
        self.mock_repo.resume.get_structured_resume_by_fingerprint.return_value = None

        with self.assertRaisesRegex(ValueError, "Structured resume missing"):
            self.service.embed_resume_one(self.mock_repo, "fp-none")

    @patch("etl.orchestrator.JobRepositoryAdapter")
    @patch("etl.orchestrator.ResumeProfiler")
    def test_embed_resume_one_requires_section_payload(self, mock_profiler_cls, _mock_adapter_cls):
        self.mock_repo.resume.get_structured_resume_by_fingerprint.return_value = MagicMock(
            extracted_data={"profile": {"summary": {"text": "Summary"}}}
        )
        with patch("etl.orchestrator.ResumeSchema.model_validate", return_value=MagicMock()):
            mock_profiler_cls.return_value.profile_resume.return_value = (
                MagicMock(),
                [MagicMock()],
                [],
            )
            with self.assertRaisesRegex(ValueError, "No resume section embeddings"):
                self.service.embed_resume_one(self.mock_repo, "fp-no-payload")

    @patch("etl.orchestrator.JobRepositoryAdapter")
    @patch("etl.orchestrator.ResumeProfiler")
    def test_embed_resume_one_requires_evidence_units(self, mock_profiler_cls, _mock_adapter_cls):
        self.mock_repo.resume.get_structured_resume_by_fingerprint.return_value = MagicMock(
            extracted_data={"profile": {"summary": {"text": "Summary"}}}
        )
        with patch("etl.orchestrator.ResumeSchema.model_validate", return_value=MagicMock()):
            mock_profiler_cls.return_value.profile_resume.return_value = (
                MagicMock(),
                [],
                [{"section_type": "summary"}],
            )
            with self.assertRaisesRegex(ValueError, "No resume evidence embeddings"):
                self.service.embed_resume_one(self.mock_repo, "fp-no-evidence")

    @patch("etl.orchestrator.JobRepositoryAdapter")
    @patch("etl.orchestrator.ResumeProfiler")
    def test_embed_resume_one_requires_summary_embedding(self, mock_profiler_cls, _mock_adapter_cls):
        self.mock_repo.resume.get_structured_resume_by_fingerprint.return_value = MagicMock(
            extracted_data={"profile": {"summary": {"text": "Summary"}}}
        )
        with patch("etl.orchestrator.ResumeSchema.model_validate", return_value=MagicMock()):
            mock_profiler_cls.return_value.profile_resume.return_value = (
                MagicMock(),
                [MagicMock()],
                [{"section_type": "summary"}],
            )
            self.mock_repo.get_resume_summary_embedding.return_value = None
            with self.assertRaisesRegex(ValueError, "No summary embedding found"):
                self.service.embed_resume_one(self.mock_repo, "fp-no-summary")


if __name__ == "__main__":
    unittest.main()
