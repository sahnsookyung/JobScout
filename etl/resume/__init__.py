#!/usr/bin/env python3
"""
Resume Extraction Module - ETL for resume parsing and profiling.

Handles:
- Structured resume extraction using AI
- Resume evidence unit extraction
- Section embedding generation

Note: Structured resume data uses Pydantic models from etl.schema_models.
"""

from etl.resume.profiler import ResumeProfiler
from etl.resume.models import ResumeEvidenceUnit
from etl.schema_models import ResumeSchema

__all__ = [
    'ResumeProfiler',
    'ResumeEvidenceUnit',
    'ResumeSchema',
]
