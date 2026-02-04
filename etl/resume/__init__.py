#!/usr/bin/env python3
"""
Resume Extraction Module - ETL for resume parsing and profiling.

Handles:
- Structured resume extraction using AI
- Resume evidence unit extraction
- Years of experience extraction and validation
- Section embedding generation
"""

from etl.resume.profiler import ResumeProfiler
from etl.resume.models import (
    ResumeEvidenceUnit,
    StructuredResumeProfile,
)

__all__ = [
    'ResumeProfiler',
    'ResumeEvidenceUnit',
    'StructuredResumeProfile',
]
