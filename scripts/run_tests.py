#!/usr/bin/env python3
"""
Test runner script that automatically starts Redis for integration tests.

Usage:
    python scripts/run_tests.py
    python scripts/run_tests.py --coverage
    python scripts/run_tests.py --watch
"""

import os
import sys
import subprocess
import time
import argparse
from pathlib import Path


def run_command(cmd, capture=False):
    """Run a shell command."""
    print(f"🔧 Running: {' '.join(cmd)}")
    if capture:
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.returncode, result.stdout, result.stderr
    else:
        return subprocess.run(cmd).returncode, None, None


def start_redis():
    """Start Redis test container on a different port."""
    print("\n🚀 Starting Redis test container on port 6380...")
    
    # Use port 6380 for tests to avoid conflict with production Redis on 6379
    test_port = "6380"
    
    # Check if Redis container already exists on test port
    returncode, stdout, _ = run_command(
        ["docker", "ps", "-q", "-f", "name=redis-test"],
        capture=True
    )
    
    if stdout.strip():
        print("✅ Redis test container already running on port 6380")
        return True, test_port
    
    # Check if container exists but is stopped
    returncode, stdout, _ = run_command(
        ["docker", "ps", "-a", "-q", "-f", "name=redis-test"],
        capture=True
    )
    
    if stdout.strip():
        print("🔄 Starting existing Redis test container...")
        returncode, _, _ = run_command(["docker", "start", "redis-test"])
        if returncode == 0:
            time.sleep(2)
            print("✅ Redis test container started on port 6380")
            return True, test_port
        else:
            print("⚠️  Failed to start existing container, removing...")
            run_command(["docker", "rm", "-f", "redis-test"])
    
    # Start new container on port 6380
    print("📦 Creating new Redis test container on port 6380...")
    returncode, _, _ = run_command([
        "docker", "run", "-d",
        "-p", f"{test_port}:6379",
        "--name", "redis-test",
        "--rm",  # Auto-remove on stop
        "redis:7-alpine"
    ])
    
    if returncode != 0:
        print("❌ Failed to start Redis test container")
        return False, test_port
    
    # Wait for Redis to be ready
    print("⏳ Waiting for Redis to be ready...")
    time.sleep(3)
    
    # Verify Redis is running
    returncode, stdout, _ = run_command(
        ["docker", "exec", "redis-test", "redis-cli", "ping"],
        capture=True
    )
    
    if stdout.strip() == "PONG":
        print("✅ Redis test container is ready on port 6380!")
        return True, test_port
    else:
        print("❌ Redis failed to start properly")
        return False, test_port


def stop_redis():
    """Stop Redis test container."""
    print("\n🛑 Stopping Redis test container...")
    returncode, _, _ = run_command(["docker", "stop", "redis-test"])
    if returncode == 0:
        print("✅ Redis container stopped")
    else:
        print("⚠️  Failed to stop Redis container (may already be stopped)")


def check_redis_available(port="6379"):
    """Check if Redis is available."""
    try:
        import redis
        conn = redis.Redis.from_url(f"redis://localhost:{port}/1", socket_timeout=2)
        try:
            return conn.ping()
        finally:
            conn.close()
    except Exception:
        return False


def run_tests(coverage=False, watch=False, test_path="tests/", redis_port="6379"):
    """Run pytest with optional coverage."""
    print("\n" + "="*60)
    print("🧪 Running Tests")
    print("="*60)
    
    # Build pytest command
    cmd = [
        "uv", "run", "pytest",
        test_path,
        "-v",
        "--tb=short",
        "--ignore=tests/integration/test_full_pipeline.py",
        "--ignore=tests/integration/test_etl_real_data.py",
        "--ignore=tests/integration/test_openai_schema_validation.py",
        "--ignore=tests/integration/test_resume_schema.py",
        "--ignore=tests/integration/test_user_wants_pipeline.py",
    ]
    
    if coverage:
        print("📊 Coverage enabled")
        cmd.extend([
            "--cov=services",
            "--cov=core",
            "--cov=database",
            "--cov=etl",
            "--cov=pipeline",
            "--cov=notification",
            "--cov=web",
            "--cov=modal",
            "--cov-report=term",
        ])

        if not watch:
            cmd.extend([
                "--cov-report=xml:coverage.xml",
                "--cov-report=html:htmlcov",
            ])
    
    if watch:
        # Use pytest-watch for watch mode
        print("👁️  Watch mode enabled - using pytest-watch")
        cmd = ["uv", "run", "ptw", "--", test_path, "-v", "--tb=short",
               "--ignore=tests/integration/test_full_pipeline.py",
               "--ignore=tests/integration/test_etl_real_data.py",
               "--ignore=tests/integration/test_openai_schema_validation.py",
               "--ignore=tests/integration/test_resume_schema.py",
               "--ignore=tests/integration/test_user_wants_pipeline.py"]
    
    # Set environment for tests
    env = os.environ.copy()
    env["REDIS_URL"] = f"redis://localhost:{redis_port}/1"
    env["PYTHONPATH"] = str(Path.cwd())
    
    # Run tests
    print(f"\n📝 Command: {' '.join(cmd)}\n")
    print(f"📍 Using Redis on port {redis_port}\n")
    returncode = subprocess.run(cmd, env=env).returncode
    
    return returncode == 0


def main():
    parser = argparse.ArgumentParser(description="Run tests with Redis")
    parser.add_argument("--coverage", "-c", action="store_true",
                       help="Generate coverage report")
    parser.add_argument("--watch", "-w", action="store_true",
                       help="Watch for changes and re-run tests")
    parser.add_argument("--no-cleanup", action="store_true",
                       help="Don't stop Redis after tests")
    parser.add_argument("--path", "-p", default="tests/",
                       help="Test path to run (default: tests/)")
    
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("🚀 JobScout Test Runner")
    print("="*60)
    
    # First check if production Redis is available on port 6379
    if check_redis_available("6379"):
        print("✅ Production Redis found on port 6379 - using it for tests")
        redis_started = False  # Don't cleanup production Redis
        redis_port = "6379"
    else:
        # Check if test Redis is available on port 6380
        if check_redis_available("6380"):
            print("✅ Test Redis found on port 6380 - using it for tests")
            redis_started = False
            redis_port = "6380"
        else:
            # Start Redis test container on port 6380
            redis_started, redis_port = start_redis()
            
            if not redis_started:
                print("\n❌ Failed to start Redis test container")
                print("\n💡 Solutions:")
                print("   1. Install Docker: https://docs.docker.com/get-docker/")
                print("   2. Start Redis manually: docker run -d -p 6380:6379 --name redis-test redis:7-alpine")
                print("   3. Run unit tests only: uv run pytest tests/unit/services/test_orchestrator.py -v")
                sys.exit(1)
    
    try:
        # Run tests
        success = run_tests(
            coverage=args.coverage,
            watch=args.watch,
            test_path=args.path,
            redis_port=redis_port
        )
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    finally:
        # Cleanup only if we started a NEW test Redis container
        # Don't cleanup if Redis was already running (production or test)
        if not args.no_cleanup and not args.watch and redis_started:
            stop_redis()
        elif redis_port == "6380" and not redis_started:
            # We used existing test Redis, remind user they can stop it
            print("\n💡 Test Redis container still running on port 6380")
            print("   To stop it: docker stop redis-test")


if __name__ == "__main__":
    main()
