"""Unit tests for etl/resume/parser.py"""

import json
import pytest
import tempfile
import os
from unittest.mock import MagicMock, patch

from etl.resume.parser import ResumeParser


@pytest.fixture
def parser():
    return ResumeParser()


def write_temp(suffix, content, mode="w", encoding="utf-8"):
    """Write content to a temp file and return path."""
    f = tempfile.NamedTemporaryFile(suffix=suffix, mode=mode, encoding=encoding, delete=False)
    f.write(content)
    f.close()
    return f.name


# ---------------------------------------------------------------------------
# parse() — routing and happy paths
# ---------------------------------------------------------------------------

class TestParseJson:
    def test_valid_json(self, parser):
        data = {"name": "Alice", "skills": ["Python", "Go"]}
        path = write_temp(".json", json.dumps(data))
        try:
            result = parser.parse(path)
            assert result.format == "json"
            assert result.data == data
            assert '"name": "Alice"' in result.text
            assert result.source_path == path
        finally:
            os.unlink(path)

    def test_invalid_json_raises_value_error(self, parser):
        path = write_temp(".json", "{ not valid json }")
        try:
            with pytest.raises(ValueError, match="Invalid JSON"):
                parser.parse(path)
        finally:
            os.unlink(path)

    def test_empty_object_json(self, parser):
        path = write_temp(".json", "{}")
        try:
            result = parser.parse(path)
            assert result.data == {}
        finally:
            os.unlink(path)


class TestParseYaml:
    def test_valid_yaml(self, parser):
        yaml_content = "name: Bob\nskills:\n  - Python\n  - Rust\n"
        path = write_temp(".yaml", yaml_content)
        try:
            result = parser.parse(path)
            assert result.format == "yaml"
            assert result.data["name"] == "Bob"
            assert "Python" in result.data["skills"]
        finally:
            os.unlink(path)

    def test_yml_extension(self, parser):
        yaml_content = "name: Carol\n"
        path = write_temp(".yml", yaml_content)
        try:
            result = parser.parse(path)
            assert result.format == "yaml"
        finally:
            os.unlink(path)

    def test_invalid_yaml_raises_value_error(self, parser):
        path = write_temp(".yaml", "key: [\nunot closed")
        try:
            with pytest.raises(ValueError, match="Invalid YAML"):
                parser.parse(path)
        finally:
            os.unlink(path)

    def test_non_dict_yaml_raises_value_error(self, parser):
        """YAML that parses to a list (not dict) should raise ValueError."""
        path = write_temp(".yaml", "- item1\n- item2\n")
        try:
            with pytest.raises(ValueError, match="mapping"):
                parser.parse(path)
        finally:
            os.unlink(path)


class TestParseTxt:
    def test_valid_txt(self, parser):
        content = "Alice Smith\nSoftware Engineer\n5 years Python experience"
        path = write_temp(".txt", content)
        try:
            result = parser.parse(path)
            assert result.format == "txt"
            assert result.data is None
            assert "Alice Smith" in result.text
        finally:
            os.unlink(path)

    def test_empty_txt_raises_value_error(self, parser):
        path = write_temp(".txt", "   \n  ")
        try:
            with pytest.raises(ValueError, match="Empty resume"):
                parser.parse(path)
        finally:
            os.unlink(path)


class TestParseDocx:
    def test_valid_docx(self, parser):
        mock_doc = MagicMock()
        para1 = MagicMock()
        para1.text = "Alice Smith"
        para2 = MagicMock()
        para2.text = "  "  # blank, should be skipped
        para3 = MagicMock()
        para3.text = "Software Engineer"
        mock_doc.paragraphs = [para1, para2, para3]

        # Table with one row and two cells
        cell1 = MagicMock()
        cell1.text = "Python"
        cell2 = MagicMock()
        cell2.text = "Docker"
        row = MagicMock()
        row.cells = [cell1, cell2]
        table = MagicMock()
        table.rows = [row]
        mock_doc.tables = [table]

        path = write_temp(".docx", "placeholder", mode="w")
        try:
            with patch("etl.resume.parser.Document", return_value=mock_doc):
                result = parser.parse(path)

            assert result.format == "docx"
            assert result.data is None
            assert "Alice Smith" in result.text
            assert "Software Engineer" in result.text
            assert "Python" in result.text
        finally:
            os.unlink(path)

    def test_empty_docx_logs_warning(self, parser):
        mock_doc = MagicMock()
        mock_doc.paragraphs = []
        mock_doc.tables = []

        path = write_temp(".docx", "placeholder", mode="w")
        try:
            with patch("etl.resume.parser.Document", return_value=mock_doc):
                result = parser.parse(path)
            assert result.text == ""
        finally:
            os.unlink(path)

    def test_docx_parse_error_raises_value_error(self, parser):
        path = write_temp(".docx", "placeholder", mode="w")
        try:
            with patch("etl.resume.parser.Document", side_effect=Exception("corrupt")):
                with pytest.raises(ValueError, match="Failed to parse DOCX"):
                    parser.parse(path)
        finally:
            os.unlink(path)


class TestParsePdf:
    def test_valid_pdf(self, parser):
        mock_reader = MagicMock()
        page1 = MagicMock()
        page1.extract_text.return_value = "Alice Smith\nSoftware Engineer"
        page2 = MagicMock()
        page2.extract_text.return_value = "  5 years Python  "
        mock_reader.pages = [page1, page2]

        path = write_temp(".pdf", "placeholder", mode="w")
        try:
            with patch("etl.resume.parser.PdfReader", return_value=mock_reader):
                result = parser.parse(path)

            assert result.format == "pdf"
            assert result.data is None
            assert "Alice Smith" in result.text
        finally:
            os.unlink(path)

    def test_empty_pdf_logs_warning(self, parser):
        mock_reader = MagicMock()
        page = MagicMock()
        page.extract_text.return_value = "  "
        mock_reader.pages = [page]

        path = write_temp(".pdf", "placeholder", mode="w")
        try:
            with patch("etl.resume.parser.PdfReader", return_value=mock_reader):
                result = parser.parse(path)
            assert result.text == ""
        finally:
            os.unlink(path)

    def test_no_pages_raises_value_error(self, parser):
        mock_reader = MagicMock()
        mock_reader.pages = []

        path = write_temp(".pdf", "placeholder", mode="w")
        try:
            with patch("etl.resume.parser.PdfReader", return_value=mock_reader):
                with pytest.raises(ValueError, match="no pages"):
                    parser.parse(path)
        finally:
            os.unlink(path)

    def test_page_extraction_error_skipped(self, parser):
        """If one page fails, extraction continues with remaining pages."""
        mock_reader = MagicMock()
        page1 = MagicMock()
        page1.extract_text.side_effect = Exception("PDF error")
        page2 = MagicMock()
        page2.extract_text.return_value = "Good content"
        mock_reader.pages = [page1, page2]

        path = write_temp(".pdf", "placeholder", mode="w")
        try:
            with patch("etl.resume.parser.PdfReader", return_value=mock_reader):
                result = parser.parse(path)
            assert "Good content" in result.text
        finally:
            os.unlink(path)

    def test_pdf_exception_raises_value_error(self, parser):
        path = write_temp(".pdf", "placeholder", mode="w")
        try:
            with patch("etl.resume.parser.PdfReader", side_effect=Exception("corrupt")):
                with pytest.raises(ValueError, match="Failed to parse PDF"):
                    parser.parse(path)
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# parse() — error cases
# ---------------------------------------------------------------------------

class TestParseErrors:
    def test_file_not_found(self, parser):
        with pytest.raises(FileNotFoundError, match="not found"):
            parser.parse("/nonexistent/path/resume.json")

    def test_unsupported_format(self, parser):
        path = write_temp(".rtf", "content")
        try:
            with pytest.raises(ValueError, match="Unsupported resume format"):
                parser.parse(path)
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# is_supported
# ---------------------------------------------------------------------------

class TestIsSupported:
    @pytest.mark.parametrize("ext", [".json", ".yaml", ".yml", ".txt", ".docx", ".pdf"])
    def test_supported_formats(self, parser, ext):
        assert parser.is_supported(f"resume{ext}") is True

    @pytest.mark.parametrize("ext", [".rtf", ".odt", ".xlsx", ".csv", ""])
    def test_unsupported_formats(self, parser, ext):
        assert parser.is_supported(f"resume{ext}") is False

    def test_case_insensitive(self, parser):
        assert parser.is_supported("RESUME.JSON") is True
        assert parser.is_supported("resume.PDF") is True


# ---------------------------------------------------------------------------
# get_supported_formats
# ---------------------------------------------------------------------------

class TestGetSupportedFormats:
    def test_returns_sorted_list(self):
        formats = ResumeParser.get_supported_formats()
        assert isinstance(formats, list)
        assert formats == sorted(formats)

    def test_includes_all_expected(self):
        formats = ResumeParser.get_supported_formats()
        for ext in [".json", ".yaml", ".yml", ".txt", ".docx", ".pdf"]:
            assert ext in formats
