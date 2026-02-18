#!/usr/bin/env python3
"""Demo script to test the multi-format resume parser.

Run with: uv run python scripts/tests/test_parser_demo.py <path-to-resume>
"""
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from etl.resume.parser import ResumeParser


def main():
    if len(sys.argv) < 2:
        print("Usage: uv run python scripts/tests/test_parser_demo.py <path-to-resume>")
        print("\nSupported formats:")
        for fmt in ResumeParser.get_supported_formats():
            print(f"  {fmt}")
        sys.exit(1)

    file_path = sys.argv[1]

    print(f"\n{'='*60}")
    print(f"Resume Parser Demo")
    print(f"{'='*60}")
    print(f"File: {file_path}")

    parser = ResumeParser()

    # Check if format is supported
    if not parser.is_supported(file_path):
        supported = ', '.join(ResumeParser.get_supported_formats())
        print(f"\n❌ Unsupported format!")
        print(f"Supported formats: {supported}")
        sys.exit(1)

    # Parse the file
    try:
        result = parser.parse(file_path)

        print(f"\n✅ Successfully parsed!")
        print(f"Format detected: {result.format}")
        print(f"Source: {result.source_path}")

        if result.data is not None:
            print(f"\n📊 Structured data available: YES")
            print(f"   Data keys: {list(result.data.keys())}")
            if 'name' in result.data:
                print(f"   Name: {result.data.get('name')}")
            if 'title' in result.data:
                print(f"   Title: {result.data.get('title')}")
        else:
            print(f"\n📄 Structured data available: NO (text-only format)")
            print(f"   Text will be processed by LLM for extraction")

        print(f"\n📝 Extracted text preview:")
        print(f"{'-'*60}")
        preview = result.text[:500] if len(result.text) > 500 else result.text
        print(preview)
        if len(result.text) > 500:
            print(f"\n... ({len(result.text) - 500} more characters)")
        print(f"{'-'*60}")

    except FileNotFoundError:
        print(f"\n❌ File not found: {file_path}")
        sys.exit(1)
    except ValueError as e:
        print(f"\n❌ Parse error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
