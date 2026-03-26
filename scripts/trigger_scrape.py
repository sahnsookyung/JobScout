#!/usr/bin/env python3
"""
Manual scrape trigger CLI for ops/admin use.

Triggers the orchestrator's scrape functionality via HTTP.
This is an alternative to the scheduled scraping that runs automatically.

Usage:
    python scripts/trigger_scrape.py [--url http://localhost:8084]

Environment:
    ORCHESTRATOR_URL: Base URL of orchestrator service (default: http://localhost:8084)
"""

import argparse
import os
import sys

import httpx


def trigger_scrape(url: str) -> int:
    """Trigger scrape and return exit code."""
    endpoint = f"{url}/orchestrate/scrape"
    
    print(f"Triggering scrape at {endpoint}...")
    
    try:
        with httpx.Client(timeout=60.0) as client:
            response = client.post(endpoint)
            
            if response.status_code == 200:
                data = response.json()
                print(f"Success: {data.get('message', 'Scrape triggered')}")
                print(f"Total jobs: {data.get('total_jobs', 0)}")
                
                if data.get('errors'):
                    print(f"Errors: {data['errors']}")
                    return 1
                
                return 0
            else:
                print(f"Error: HTTP {response.status_code}")
                print(f"Response: {response.text}")
                return 1
                
    except httpx.ConnectError as e:
        print(f"Connection error: Could not connect to {endpoint}")
        print(f"Make sure the orchestrator service is running.")
        return 1
    except httpx.TimeoutException:
        print(f"Timeout: Scrape request timed out")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        return 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Manually trigger a scrape run for all configured scrapers."
    )
    parser.add_argument(
        "--url",
        default=os.getenv("ORCHESTRATOR_URL", "http://localhost:8084"),
        help="Orchestrator service URL (default: ORCHESTRATOR_URL env var or http://localhost:8084)",
    )
    
    args = parser.parse_args()
    return trigger_scrape(args.url)


if __name__ == "__main__":
    sys.exit(main())
