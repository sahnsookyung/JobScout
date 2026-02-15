#!/usr/bin/env python3
"""Resume extraction test script - no DB writes, full logs."""

import argparse
import json
import logging
import os
import sys

+sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.config_loader import load_config
from core.app_context import AppContext
from etl.resume import ResumeProfiler
from database.models import generate_resume_fingerprint


def setup_logging(level: int = logging.DEBUG):
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        force=True
    )


def main():
    parser = argparse.ArgumentParser(description="Run resume extraction without DB writes")
    parser.add_argument(
        '--resume',
        type=str,
        default='resume.json',
        help='Path to resume JSON file (default: resume.json)'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        default='DEBUG',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: DEBUG)'
    )
    args = parser.parse_args()

    log_level = getattr(logging, args.log_level.upper())
    setup_logging(log_level)

    logger = logging.getLogger(__name__)
    logger.info(f"Starting resume extraction test (no DB writes)")
    logger.info(f"Resume file: {args.resume}")

    if not os.path.exists(args.resume):
        logger.error(f"Resume file not found: {args.resume}")
        sys.exit(1)

    try:
        with open(args.resume, 'r', encoding='utf-8') as f:
            resume_data = json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in resume file: {e}")
        sys.exit(1)

    fingerprint = generate_resume_fingerprint(resume_data)
    logger.info(f"Resume fingerprint: {fingerprint[:16]}...")

    config = load_config()
    ctx = AppContext.build(config)

    profiler = ResumeProfiler(ai_service=ctx.ai_service)

    logger.info("Running resume profiling (no store - no DB writes)...")
    resume, evidence_units, persistence_payload = profiler.profile_resume(resume_data)

    if resume:
        logger.info(f"=== EXTRACTION COMPLETE ===")
        logger.info(f"Total experience: {resume.claimed_total_years} years")
        logger.info(f"Confidence: {resume.extraction.confidence if resume.extraction else 'N/A'}")
        
        logger.info(f"\n=== PROFILE SUMMARY ===")
        logger.info(f"Summary: {resume.profile.summary.text[:200] if resume.profile.summary and resume.profile.summary.text else 'N/A'}...")
        logger.info(f"Experience entries: {len(resume.profile.experience)}")
        logger.info(f"Education entries: {len(resume.profile.education) if resume.profile.education else 0}")
        logger.info(f"Skills: {len(resume.profile.skills.all) if resume.profile.skills else 0}")
        
        logger.info(f"\n=== EVIDENCE UNITS ===")
        logger.info(f"Total evidence units: {len(evidence_units)}")
        
        units_by_section = {}
        for unit in evidence_units:
            section = unit.source_section
            units_by_section[section] = units_by_section.get(section, 0) + 1
        
        for section, count in units_by_section.items():
            logger.info(f"  {section}: {count} units")
        
        units_with_years = [u for u in evidence_units if u.years_value is not None]
        logger.info(f"Evidence units with years: {len(units_with_years)}")
        
        logger.info(f"\n=== PERSISTENCE PAYLOAD ===")
        logger.info(f"Section embeddings generated: {len(persistence_payload)}")
        
        logger.info(f"\n=== FULL EXTRACTED DATA ===")
        logger.info(json.dumps(resume.model_dump(), indent=2))
    else:
        logger.error("Failed to extract resume data")
        sys.exit(1)


if __name__ == "__main__":
    main()
