#!/usr/bin/env python3
"""
Diagnostic script to check microservices connectivity and pipeline status.

Usage:
    python scripts/diagnose_pipeline.py
"""

import os
import sys
import httpx
import json

# Colors for output
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

def print_header(text):
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'=' * 60}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'=' * 60}{Colors.RESET}\n")

def print_success(text):
    print(f"{Colors.GREEN}✅ {text}{Colors.RESET}")

def print_error(text):
    print(f"{Colors.RED}❌ {text}{Colors.RESET}")

def print_warning(text):
    print(f"{Colors.YELLOW}⚠️  {text}{Colors.RESET}")

def print_info(text):
    print(f"{Colors.BLUE}ℹ️  {text}{Colors.RESET}")

def check_service_health(name, url):
    """Check if a service is healthy."""
    try:
        with httpx.Client(timeout=5.0) as client:
            response = client.get(f"{url}/health")
            if response.status_code == 200:
                data = response.json()
                print_success(f"{name}: {data.get('status', 'unknown')}")
                return True, data
            else:
                print_error(f"{name}: HTTP {response.status_code}")
                return False, None
    except httpx.ConnectError:
        print_error(f"{name}: Connection failed")
        return False, None
    except Exception:
        print_error(f"{name}: Error")
        return False, None

def check_orchestrator_diagnostics(url):
    """Get orchestrator diagnostics."""
    try:
        with httpx.Client(timeout=10.0) as client:
            response = client.get(f"{url}/orchestrate/diagnostics")
            if response.status_code == 200:
                data = response.json()
                print_success("Orchestrator diagnostics retrieved")
                
                # Print stream info
                print("\n" + "-" * 40)
                print("Redis Streams Status:")
                print("-" * 40)
                streams = data.get('streams', {})
                for stream_name, info in streams.items():
                    exists = info.get('exists', False)
                    length = info.get('length', 0)
                    status = f"{Colors.GREEN}EXISTS{Colors.RESET}" if exists else f"{Colors.RED}MISSING{Colors.RESET}"
                    print(f"  {stream_name}: {status} (length: {length})")
                    
                    if 'consumer_groups' in info:
                        for cg in info['consumer_groups']:
                            print(f"    └─ Group: {cg.get('name')}")
                            print(f"       Consumers: {cg.get('consumers', 0)}, Pending: {cg.get('pending', 0)}")
                
                # Print active orchestrations
                print("\n" + "-" * 40)
                print("Active Orchestrations:")
                print("-" * 40)
                active = data.get('active_orchestrations', [])
                if active:
                    for task in active:
                        print(f"  {task.get('task_id')}: {task.get('status')}")
                else:
                    print_info("No active orchestrations")
                
                return True, data
            else:
                print_error(f"Orchestrator diagnostics: HTTP {response.status_code}")
                return False, None
    except Exception as e:
        print_error(f"Orchestrator diagnostics: {e}")
        return False, None

def test_pipeline_trigger(url):
    """Test triggering the pipeline."""
    try:
        with httpx.Client(timeout=10.0) as client:
            print_info("Triggering pipeline...")
            response = client.post(f"{url}/orchestrate/match", json={})
            if response.status_code == 200:
                data = response.json()
                print_success(f"Pipeline triggered: task_id={data.get('task_id')}")
                return True, data
            else:
                print_error(f"Pipeline trigger: HTTP {response.status_code} - {response.text}")
                return False, None
    except Exception as e:
        print_error(f"Pipeline trigger: {e}")
        return False, None

# pylint: disable=too-many-branches
def main():
    print_header("MICROSERVICES PIPELINE DIAGNOSTICS")

    # Get base URLs from environment or use defaults
    base_url = os.getenv('ORCHESTRATOR_URL', 'http://localhost:8084')
    extraction_url = os.getenv('EXTRACTION_URL', 'http://localhost:8081')
    embeddings_url = os.getenv('EMBEDDINGS_URL', 'http://localhost:8082')
    matcher_url = os.getenv('SCORER_MATCHER_URL', 'http://localhost:8083')

    print_info(f"Using orchestrator URL: {base_url}")
    print_info(f"Using extraction URL: {extraction_url}")
    print_info(f"Using embeddings URL: {embeddings_url}")
    print_info(f"Using matcher URL: {matcher_url}")

    # Check all services health
    print_header("SERVICE HEALTH CHECKS")

    services = [
        ("Extraction", extraction_url),
        ("Embeddings", embeddings_url),
        ("Scorer-Matcher", matcher_url),
        ("Orchestrator", base_url),
    ]

    healthy_services = []
    for name, url in services:
        success, _ = check_service_health(name, url)
        if success:
            healthy_services.append((name, url))

    # Check orchestrator diagnostics
    print_header("ORCHESTRATOR DIAGNOSTICS")
    success, _ = check_orchestrator_diagnostics(base_url)

    if success:
        # Optionally test pipeline trigger
        print_header("PIPELINE TRIGGER TEST")
        print_warning("This will trigger a real pipeline run. Continue? (y/n)")
        try:
            choice = input("> ").strip().lower()
            if choice == 'y':
                test_pipeline_trigger(base_url)
        except (KeyboardInterrupt, EOFError):
            pass

    # Summary
    print_header("SUMMARY")
    print(f"Healthy services: {len(healthy_services)}/{len(services)}")
    for name, _ in healthy_services:
        print_success(f"  {name}")

    unhealthy = [name for name, _ in services if name not in [h[0] for h in healthy_services]]
    if unhealthy:
        print_error(f"Unhealthy services: {', '.join(unhealthy)}")
        print("\nTroubleshooting steps:")
        print("1. Check if Docker containers are running: docker-compose ps")
        print("2. Check service logs: docker-compose logs <service-name>")
        print("3. Verify network connectivity between containers")
        print("4. Check Redis connectivity: docker-compose exec redis redis-cli ping")

    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()
