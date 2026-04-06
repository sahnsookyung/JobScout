"""Unit tests for etl/resume/loader.py"""

from types import SimpleNamespace
from unittest.mock import patch

from etl.resume.loader import load_resume_with_parser


def _make_parsed(data=None, text=""):
    return SimpleNamespace(data=data, text=text)


class TestLoadResumeWithParser:
    def test_returns_parsed_data_when_present(self):
        parsed = _make_parsed(data={"name": "Alice"})
        with patch("etl.resume.loader.ResumeParser") as MockParser:
            MockParser.return_value.parse.return_value = parsed
            result = load_resume_with_parser("/resume.json")
        assert result == {"name": "Alice"}

    def test_returns_raw_text_dict_when_data_is_none(self):
        parsed = _make_parsed(data=None, text="Alice Engineer")
        with patch("etl.resume.loader.ResumeParser") as MockParser:
            MockParser.return_value.parse.return_value = parsed
            result = load_resume_with_parser("/resume.txt")
        assert result == {"raw_text": "Alice Engineer"}

    def test_returns_none_on_file_not_found(self):
        with patch("etl.resume.loader.ResumeParser") as MockParser:
            MockParser.return_value.parse.side_effect = FileNotFoundError("missing")
            result = load_resume_with_parser("/no/such/file.pdf")
        assert result is None

    def test_returns_none_on_value_error(self):
        with patch("etl.resume.loader.ResumeParser") as MockParser:
            MockParser.return_value.parse.side_effect = ValueError("bad format")
            result = load_resume_with_parser("/bad.xyz")
        assert result is None

    def test_returns_none_on_unexpected_exception(self):
        with patch("etl.resume.loader.ResumeParser") as MockParser:
            MockParser.return_value.parse.side_effect = RuntimeError("unexpected")
            result = load_resume_with_parser("/resume.pdf")
        assert result is None
