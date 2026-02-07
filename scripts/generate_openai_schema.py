#!/usr/bin/env python3
"""
Generate OpenAI-compatible JSON Schema from resume/job models.

This script outputs the full schema for OpenAI's structured outputs feature.
This isn't used for anything in the current pipeline, but the visual validation of the schema proves useful for sanity checks.
Run with: python scripts/generate_openai_schema.py 

Output formats:
- --json: Full JSON schema (for OpenAI API)
- --markdown: Human-readable markdown documentation

Example usage:
    python scripts/generate_openai_schema.py --schema resume --format json
    python scripts/generate_openai_schema.py --schema job --format markdown
    python scripts/generate_openai_schema.py --schema all --indent 4
"""
import argparse
import json
import sys
from pathlib import Path

# Ensure we can import from the project root
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from core.llm.schema_models import (
    ResumeSchema,
    RESUME_SCHEMA,
    EXTRACTION_SCHEMA,
    FACET_EXTRACTION_SCHEMA_FOR_WANTS,
)


def format_json(data: dict, indent: int = 2) -> str:
    """Format dict as JSON string."""
    return json.dumps(data, indent=indent, ensure_ascii=False)


def print_schema_info(schema: dict, name: str) -> None:
    """Print schema metadata."""
    print(f"\n{'=' * 60}")
    print(f"Schema: {name}")
    print(f"Strict Mode: {schema['strict']}")
    print(f"{'=' * 60}\n")


def generate_markdown_for_schema(schema: dict, name: str) -> str:
    """Generate human-readable markdown for a schema."""
    md = []
    md.append(f"## {name}\n")
    md.append(f"- **Name**: `{schema['name']}`\n")
    md.append(f"- **Strict**: `{schema['strict']}`\n\n")

    # Schema may already be a dict or need JSON parsing
    schema_data = schema['schema']
    if isinstance(schema_data, str):
        full_schema = json.loads(schema_data)
    else:
        full_schema = schema_data

    # Top-level properties
    if 'properties' in full_schema:
        md.append("### Top-Level Fields\n\n")
        for field, props in full_schema['properties'].items():
            desc = props.get('description', 'No description')
            md.append(f"- `{field}`: {desc}\n")
        md.append("\n")

    return "".join(md)


def main():
    parser = argparse.ArgumentParser(
        description="Generate OpenAI-compatible JSON Schema from resume/job models",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        '--schema',
        choices=['resume', 'job', 'facet', 'all'],
        default='all',
        help='Which schema to generate (default: all)'
    )
    parser.add_argument(
        '--format',
        choices=['json', 'markdown'],
        default='json',
        help='Output format (default: json)'
    )
    parser.add_argument(
        '--indent',
        type=int,
        default=2,
        help='JSON indent spaces (default: 2)'
    )
    parser.add_argument(
        '--no-header',
        action='store_true',
        help='Skip printing schema headers'
    )

    args = parser.parse_args()

    schemas = {
        'resume': ('Resume Schema', RESUME_SCHEMA),
        'job': ('Job Extraction Schema', EXTRACTION_SCHEMA),
        'facet': ('Job Facet Schema', FACET_EXTRACTION_SCHEMA_FOR_WANTS),
    }

    output = []

    if args.schema == 'all':
        for key, (label, schema) in schemas.items():
            if args.format == 'markdown':
                output.append(generate_markdown_for_schema(schema, label))
            else:
                if not args.no_header:
                    print_schema_info(schema, label)
                print(format_json(schema, args.indent))
    else:
        label, schema = schemas[args.schema]
        if args.format == 'markdown':
            output.append(generate_markdown_for_schema(schema, label))
        else:
            if not args.no_header:
                print_schema_info(schema, label)
            print(format_json(schema, args.indent))

    # Print accumulated markdown output to stdout
    if output:
        print("".join(output))


if __name__ == "__main__":
    main()
