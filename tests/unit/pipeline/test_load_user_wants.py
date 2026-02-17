#!/usr/bin/env python3
"""
Unit tests for user wants file loading functionality.

Tests the load_user_wants_data() function for edge cases including:
- File not found
- Empty files
- Files with blank lines
- Files with whitespace handling
- Unicode content
- Very long lines
"""

import unittest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch

from pipeline.runner import load_user_wants_data


class TestLoadUserWantsData(unittest.TestCase):
    """Tests for load_user_wants_data function."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_wants_file(self, content: str, filename: str = "wants.txt") -> str:
        """Helper to create a temporary wants file."""
        file_path = os.path.join(self.temp_dir, filename)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        return file_path

    def test_load_basic_wants(self):
        """Should load wants from a basic file with one per line."""
        content = "I want remote work\nLooking for Python roles\nGood work-life balance"
        file_path = self._create_wants_file(content)
        
        result = load_user_wants_data(file_path)
        
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], "I want remote work")
        self.assertEqual(result[1], "Looking for Python roles")
        self.assertEqual(result[2], "Good work-life balance")

    def test_load_empty_file(self):
        """Empty file should return empty list."""
        file_path = self._create_wants_file("")
        
        result = load_user_wants_data(file_path)
        
        self.assertEqual(result, [])

    def test_load_blank_lines_filtered(self):
        """Blank lines should be filtered out."""
        content = "Want 1\n\n\nWant 2\n\nWant 3"
        file_path = self._create_wants_file(content)
        
        result = load_user_wants_data(file_path)
        
        self.assertEqual(len(result), 3)
        self.assertEqual(result, ["Want 1", "Want 2", "Want 3"])

    def test_load_whitespace_stripping(self):
        """Leading/trailing whitespace should be stripped."""
        content = "  Want with leading spaces  \n\tWant with tabs\t\nWant with no extra space"
        file_path = self._create_wants_file(content)
        
        result = load_user_wants_data(file_path)
        
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], "Want with leading spaces")
        self.assertEqual(result[1], "Want with tabs")
        self.assertEqual(result[2], "Want with no extra space")

    def test_load_unicode_content(self):
        """Should handle Unicode content correctly."""
        content = "日本語の仕事\nTrabajo en español\nРабота на русском"
        file_path = self._create_wants_file(content)
        
        result = load_user_wants_data(file_path)
        
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], "日本語の仕事")
        self.assertEqual(result[1], "Trabajo en español")
        self.assertEqual(result[2], "Работа на русском")

    def test_load_single_want(self):
        """Should handle file with single want."""
        content = "Only one want here"
        file_path = self._create_wants_file(content)
        
        result = load_user_wants_data(file_path)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "Only one want here")

    def test_load_file_not_found(self):
        """Missing file should return empty list and log warning."""
        nonexistent_path = "/nonexistent/path/wants.txt"
        
        with self.assertLogs('pipeline.runner', level='WARNING') as log_context:
            result = load_user_wants_data(nonexistent_path)
        
        self.assertEqual(result, [])
        self.assertIn("not found", log_context.output[0].lower())

    def test_load_file_with_comments(self):
        """Should include lines that look like comments (no special handling)."""
        content = "# This is a comment line\nReal want here\n// Another comment"
        file_path = self._create_wants_file(content)
        
        result = load_user_wants_data(file_path)
        
        # Current implementation doesn't filter comments, just strips whitespace
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], "# This is a comment line")
        self.assertEqual(result[1], "Real want here")
        self.assertEqual(result[2], "// Another comment")

    def test_load_very_long_line(self):
        """Should handle very long want lines."""
        long_want = "A" * 10000  # 10k character line
        content = f"Short want\n{long_want}\nAnother short want"
        file_path = self._create_wants_file(content)
        
        result = load_user_wants_data(file_path)
        
        self.assertEqual(len(result), 3)
        self.assertEqual(result[1], long_want)

    def test_load_special_characters(self):
        """Should handle special characters and punctuation."""
        content = "Want: something!\n$$$ High salary $$$\nC++ & C# experience"
        file_path = self._create_wants_file(content)
        
        result = load_user_wants_data(file_path)
        
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], "Want: something!")
        self.assertEqual(result[1], "$$$ High salary $$$")
        self.assertEqual(result[2], "C++ & C# experience")

    def test_load_trailing_newlines(self):
        """Should handle files with trailing newlines."""
        content = "Want 1\nWant 2\n\n\n"
        file_path = self._create_wants_file(content)
        
        result = load_user_wants_data(file_path)
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result, ["Want 1", "Want 2"])

    def test_load_windows_line_endings(self):
        """Should handle Windows-style line endings (CRLF)."""
        # Write in binary mode to ensure actual CRLF characters
        content = b"Want 1\r\nWant 2\r\nWant 3"
        file_path = os.path.join(self.temp_dir, "crlf_wants.txt")
        with open(file_path, 'wb') as f:
            f.write(content)
        
        result = load_user_wants_data(file_path)
        
        # strip() should remove \r
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], "Want 1")
        self.assertEqual(result[1], "Want 2")
        self.assertEqual(result[2], "Want 3")

    @patch('builtins.open')
    def test_load_permission_error(self, mock_open):
        """Permission error should return empty list and log error."""
        mock_open.side_effect = PermissionError("Permission denied")
        
        with self.assertLogs('pipeline.runner', level='ERROR') as log_context:
            result = load_user_wants_data("/some/path/wants.txt")
        
        self.assertEqual(result, [])
        self.assertIn("error", log_context.output[0].lower())

    def test_load_readonly_file(self):
        """Should be able to read from read-only file."""
        content = "Read-only want"
        file_path = self._create_wants_file(content)
        os.chmod(file_path, 0o444)  # Read-only
        
        try:
            result = load_user_wants_data(file_path)
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0], "Read-only want")
        finally:
            os.chmod(file_path, 0o644)  # Restore permissions for cleanup


class TestLoadUserWantsDataIntegration(unittest.TestCase):
    """Integration tests using actual files from fixtures."""

    def test_fixture_directory_structure(self):
        """Verify test fixtures directory exists and is accessible."""
        fixtures_dir = Path(__file__).parent.parent.parent / "fixtures" / "user_wants"
        if not fixtures_dir.exists():
            self.skipTest("Fixtures directory not yet created")
        self.assertTrue(fixtures_dir.is_dir())

    def test_load_sample_fixture(self):
        """Should load wants from sample fixture file."""
        fixture_path = Path(__file__).parent.parent.parent / "fixtures" / "user_wants" / "sample_wants.txt"
        if not fixture_path.exists():
            self.skipTest("Sample fixture file not found")

        result = load_user_wants_data(str(fixture_path))

        # Comments are NOT filtered - they're treated as regular wants
        # Sample fixture: 3 comments + 1 blank (filtered) + 8 actual wants = 11 total
        self.assertEqual(len(result), 11)
        self.assertEqual(result[0], "# Sample User Wants File")
        # Line 4 is blank and filtered, so first want is at index 3
        self.assertEqual(result[3], "I want a fully remote position with flexible hours")

    def test_load_minimal_fixture(self):
        """Should load wants from minimal fixture file."""
        fixture_path = Path(__file__).parent.parent.parent / "fixtures" / "user_wants" / "minimal_wants.txt"
        if not fixture_path.exists():
            self.skipTest("Minimal fixture file not found")

        result = load_user_wants_data(str(fixture_path))

        # Minimal fixture: 1 comment + 1 actual want
        self.assertEqual(len(result), 2)
        self.assertEqual(result[1], "I want remote work")

    def test_load_empty_fixture(self):
        """Should load comment from 'empty' fixture file."""
        fixture_path = Path(__file__).parent.parent.parent / "fixtures" / "user_wants" / "empty_wants.txt"
        if not fixture_path.exists():
            self.skipTest("Empty fixture file not found")

        result = load_user_wants_data(str(fixture_path))

        # Empty fixture has a comment describing it
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "# Empty file - used to test empty wants handling")

    def test_load_comprehensive_fixture(self):
        """Should load all wants from comprehensive fixture file."""
        fixture_path = Path(__file__).parent.parent.parent / "fixtures" / "user_wants" / "comprehensive_wants.txt"
        if not fixture_path.exists():
            self.skipTest("Comprehensive fixture file not found")

        result = load_user_wants_data(str(fixture_path))

        # Comprehensive fixture has many wants including comments
        self.assertGreater(len(result), 15)


if __name__ == '__main__':
    unittest.main()
