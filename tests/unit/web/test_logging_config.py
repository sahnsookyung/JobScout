#!/usr/bin/env python3
"""
Tests for logging configuration.
Covers: web/backend/logging_config.py
"""

import unittest
from web.backend.logging_config import LOGGING_CONFIG


class TestLoggingConfig(unittest.TestCase):
    """Tests for LOGGING_CONFIG structure and validity."""

    def test_config_has_required_version(self):
        """Test that config has version key."""
        self.assertIn('version', LOGGING_CONFIG)
        self.assertEqual(LOGGING_CONFIG['version'], 1)

    def test_config_has_formatters(self):
        """Test that config has formatters section."""
        self.assertIn('formatters', LOGGING_CONFIG)
        self.assertIn('default', LOGGING_CONFIG['formatters'])
        self.assertIn('access', LOGGING_CONFIG['formatters'])

    def test_config_has_handlers(self):
        """Test that config has handlers section."""
        self.assertIn('handlers', LOGGING_CONFIG)
        self.assertIn('default', LOGGING_CONFIG['handlers'])
        self.assertIn('access', LOGGING_CONFIG['handlers'])

    def test_config_has_loggers(self):
        """Test that config has loggers section."""
        self.assertIn('loggers', LOGGING_CONFIG)
        self.assertIn('uvicorn', LOGGING_CONFIG['loggers'])
        self.assertIn('uvicorn.error', LOGGING_CONFIG['loggers'])
        self.assertIn('uvicorn.access', LOGGING_CONFIG['loggers'])

    def test_config_has_root_logger(self):
        """Test that config has root logger."""
        self.assertIn('root', LOGGING_CONFIG)
        self.assertIn('level', LOGGING_CONFIG['root'])
        self.assertIn('handlers', LOGGING_CONFIG['root'])

    def test_config_disable_existing_loggers(self):
        """Test that disable_existing_loggers is set."""
        self.assertIn('disable_existing_loggers', LOGGING_CONFIG)
        self.assertFalse(LOGGING_CONFIG['disable_existing_loggers'])

    def test_formatter_has_format_and_datefmt(self):
        """Test that formatters have required keys."""
        default_formatter = LOGGING_CONFIG['formatters']['default']
        self.assertIn('format', default_formatter)
        self.assertIn('datefmt', default_formatter)
        self.assertIn('%(asctime)s', default_formatter['format'])
        self.assertIn('%(levelname)s', default_formatter['format'])

    def test_handler_has_required_keys(self):
        """Test that handlers have required keys."""
        default_handler = LOGGING_CONFIG['handlers']['default']
        self.assertIn('formatter', default_handler)
        self.assertIn('class', default_handler)
        self.assertIn('stream', default_handler)

    def test_logger_has_handlers_and_level(self):
        """Test that loggers have required keys."""
        uvicorn_logger = LOGGING_CONFIG['loggers']['uvicorn']
        self.assertIn('handlers', uvicorn_logger)
        self.assertIn('level', uvicorn_logger)
        self.assertIn('propagate', uvicorn_logger)

    def test_config_can_be_applied(self):
        """Test that logging config can be applied without errors."""
        import logging.config
        try:
            logging.config.dictConfig(LOGGING_CONFIG)
            success = True
        except Exception:
            success = False
        self.assertTrue(success)


if __name__ == '__main__':
    unittest.main()
