#!/usr/bin/env python3
"""
Unit tests for database models - resume fingerprint functions.
"""

import sys
import unittest
import xxhash
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))


class TestResumeFingerprint(unittest.TestCase):
    """Tests for resume fingerprint generation functions."""

    def test_generate_file_fingerprint(self):
        """Test generate_file_fingerprint produces correct XXH3 hash."""
        from database.models.resume import generate_file_fingerprint

        content = b'{"name": "Test User", "experience": []}'
        expected_hash = xxhash.xxh64_hexdigest(content)

        result = generate_file_fingerprint(content)

        self.assertEqual(result, expected_hash)
        self.assertEqual(len(result), 16)

    def test_generate_file_fingerprint_deterministic(self):
        """Test generate_file_fingerprint is deterministic (same input = same output)."""
        from database.models.resume import generate_file_fingerprint

        content = b'identical content for testing'

        fp1 = generate_file_fingerprint(content)
        fp2 = generate_file_fingerprint(content)

        self.assertEqual(fp1, fp2)

    def test_generate_file_fingerprint_different_content(self):
        """Test generate_file_fingerprint differs for different content."""
        from database.models.resume import generate_file_fingerprint

        content1 = b'content A'
        content2 = b'content B'

        fp1 = generate_file_fingerprint(content1)
        fp2 = generate_file_fingerprint(content2)

        self.assertNotEqual(fp1, fp2)

    def test_generate_file_fingerprint_binary_content(self):
        """Test generate_file_fingerprint works with binary content."""
        from database.models.resume import generate_file_fingerprint

        # Simulate PDF binary content
        binary_content = b'\x00\x01\x02\x03\x04\x05\xff\xfe\xfd'

        result = generate_file_fingerprint(binary_content)

        expected_hash = xxhash.xxh64_hexdigest(binary_content)
        self.assertEqual(result, expected_hash)

    def test_generate_file_fingerprint_vs_content_fingerprint(self):
        """Test that file fingerprint differs from content fingerprint."""
        import json
        from database.models.resume import generate_file_fingerprint, generate_resume_fingerprint

        content = b'{"name": "Test", "experience": []}'
        file_fp = generate_file_fingerprint(content)

        # Content-based fingerprint hashes the parsed JSON
        parsed_data = json.loads(content)
        content_fp = generate_resume_fingerprint(parsed_data)

        # Same underlying data, but different hashing strategies → different fingerprints
        self.assertNotEqual(file_fp, content_fp)

    def test_resume_hash_exists_method(self):
        """Test resume_hash_exists repository method."""
        from database.repositories.resume import ResumeRepository

        # This is a unit test that checks the method exists
        # Full integration test would require database
        self.assertTrue(hasattr(ResumeRepository, 'resume_hash_exists'))


if __name__ == '__main__':
    unittest.main()
