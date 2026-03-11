#!/usr/bin/env python3
"""
Integration Tests: Orchestrator and Microservices Pipeline

These tests start actual microservices containers to test the full integration:
1. Redis - Message broker
2. Extraction Service - Resume/job extraction
3. Embeddings Service - Vector embeddings
4. Scorer-Matcher Service - Job matching
5. Orchestrator Service - Pipeline coordination

Usage:
    uv run pytest tests/integration/test_orchestrator_pipeline.py -v
"""

import os
import uuid
import pytest
import subprocess
import time
import requests
from typing import Generator

# Configuration - Test services run on different ports to avoid conflicts
REDIS_PORT = os.environ.get("TEST_REDIS_PORT", "6380")
EXTRACTION_PORT = os.environ.get("TEST_EXTRACTION_PORT", "18081")
EMBEDDINGS_PORT = os.environ.get("TEST_EMBEDDINGS_PORT", "18082")
MATCHER_PORT = os.environ.get("TEST_MATCHER_PORT", "18083")
ORCHESTRATOR_PORT = os.environ.get("TEST_ORCHESTRATOR_PORT", "18084")

# Service URLs - use configured ports
REDIS_URL = f"redis://localhost:{REDIS_PORT}/1"
REDIS_URL_DOCKER = "redis://redis-test:6379/1"  # For containers
EXTRACTION_URL = f"http://localhost:{EXTRACTION_PORT}"
EMBEDDINGS_URL = f"http://localhost:{EMBEDDINGS_PORT}"
MATCHER_URL = f"http://localhost:{MATCHER_PORT}"
ORCHESTRATOR_URL = f"http://localhost:{ORCHESTRATOR_PORT}"

# Set environment for modules that import at load time
os.environ["REDIS_URL"] = REDIS_URL
os.environ["EXTRACTION_URL"] = EXTRACTION_URL
os.environ["EMBEDDINGS_URL"] = EMBEDDINGS_URL
os.environ["SCORER_MATCHER_URL"] = MATCHER_URL
os.environ["ORCHESTRATOR_URL"] = ORCHESTRATOR_URL


def run_docker_command(args, check=False):
    """Run docker command and return result. Raises error if check=True and command fails."""
    result = subprocess.run(
        ["docker"] + args,
        capture_output=True,
        text=True
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"Docker command failed: {' '.join(args)}\nError: {result.stderr}")
    return result


def start_test_infrastructure():
    """Start Redis and all microservices containers."""
    # Check if Docker is available
    result = run_docker_command(["version"])
    if result.returncode != 0:
        pytest.skip("Docker not available - skipping integration tests")
        return

    # Check if the required image exists
    image_check = run_docker_command(["images", "-q", "jobscout-orchestrator:latest"])
    if not image_check.stdout.strip():
        pytest.skip("Docker image 'jobscout-orchestrator:latest' not found. Please build it first before running integration tests.")

    print("\nStarting test infrastructure...")

    # Stop and remove existing containers safely
    for container in ["redis-test", "extraction-test", "embeddings-test", "matcher-test", "orchestrator-test"]:
        run_docker_command(["stop", container])
        run_docker_command(["rm", container])

    # Create test network
    run_docker_command(["network", "rm", "test-network"])
    run_docker_command(["network", "create", "test-network"], check=True)
    
    # Start Redis
    print("  Starting Redis...")
    run_docker_command([
        "run", "-d",
        "--name", "redis-test",
        "--network", "test-network",
        "-p", f"{REDIS_PORT}:6379",
        "--rm",
        "redis:7-alpine"
    ], check=True)
    
    # Wait for Redis
    wait_for_service(f"localhost:{REDIS_PORT}", "redis-cli ping")
    
    # Start Extraction Service
    print("  Starting Extraction Service...")
    run_docker_command([
        "run", "-d",
        "--name", "extraction-test",
        "--network", "test-network",
        "-p", f"{EXTRACTION_PORT}:8081",
        "-e", f"REDIS_URL={REDIS_URL_DOCKER}",
        "-e", "EXTRACTION_CONSUMER_GROUP=extraction-service",
        "-e", "HOSTNAME=extraction-test",
        "--rm",
        "jobscout-orchestrator:latest",
        "uv", "run", "uvicorn", "services.extraction.main:app",
        "--host", "0.0.0.0", "--port", "8081"
    ], check=True)
    
    # Start Embeddings Service
    print("  Starting Embeddings Service...")
    run_docker_command([
        "run", "-d",
        "--name", "embeddings-test",
        "--network", "test-network",
        "-p", f"{EMBEDDINGS_PORT}:8082",
        "-e", f"REDIS_URL={REDIS_URL_DOCKER}",
        "-e", "EMBEDDINGS_CONSUMER_GROUP=embeddings-service",
        "-e", "HOSTNAME=embeddings-test",
        "--rm",
        "jobscout-orchestrator:latest",
        "uv", "run", "uvicorn", "services.embeddings.main:app",
        "--host", "0.0.0.0", "--port", "8082"
    ], check=True)
    
    # Start Scorer-Matcher Service
    print("  Starting Scorer-Matcher Service...")
    run_docker_command([
        "run", "-d",
        "--name", "matcher-test",
        "--network", "test-network",
        "-p", f"{MATCHER_PORT}:8083",
        "-e", f"REDIS_URL={REDIS_URL_DOCKER}",
        "-e", "MATCHER_CONSUMER_GROUP=matcher-service",
        "-e", "HOSTNAME=matcher-test",
        "--rm",
        "jobscout-orchestrator:latest",
        "uv", "run", "uvicorn", "services.scorer_matcher.main:app",
        "--host", "0.0.0.0", "--port", "8083"
    ], check=True)
    
    # Start Orchestrator Service
    print("  Starting Orchestrator Service...")
    run_docker_command([
        "run", "-d",
        "--name", "orchestrator-test",
        "--network", "test-network",
        "-p", f"{ORCHESTRATOR_PORT}:8084",
        "-e", f"REDIS_URL={REDIS_URL_DOCKER}",
        "-e", "EXTRACTION_URL=http://extraction-test:8081",
        "-e", "EMBEDDINGS_URL=http://embeddings-test:8082",
        "-e", "SCORER_MATCHER_URL=http://matcher-test:8083",
        "--rm",
        "jobscout-orchestrator:latest",
        "uv", "run", "uvicorn", "services.orchestrator.main:app",
        "--host", "0.0.0.0", "--port", "8084"
    ], check=True)
    
    # Wait for all services to be healthy
    print("  Waiting for services to be ready...")
    wait_for_service(f"localhost:{EXTRACTION_PORT}", "health")
    wait_for_service(f"localhost:{EMBEDDINGS_PORT}", "health")
    wait_for_service(f"localhost:{MATCHER_PORT}", "health")
    wait_for_service(f"localhost:{ORCHESTRATOR_PORT}", "health")
    
    print("  All services ready!")


def wait_for_service(host_port, check_type, timeout=60):
    """Wait for service to be ready."""
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            if check_type == "health":
                response = requests.get(f"http://{host_port}/health", timeout=2)  # nosec: B113
                if response.status_code == 200:
                    return
            elif check_type == "redis-cli ping":
                result = run_docker_command(["exec", "redis-test", "redis-cli", "ping"])
                if result.stdout.strip() == "PONG":
                    return
        except (requests.exceptions.RequestException, ConnectionError):
            pass
        time.sleep(1)

    raise RuntimeError(f"Service at {host_port} failed to start within {timeout}s")


def stop_test_infrastructure():
    """Stop all test containers."""
    print("\nStopping test infrastructure...")
    for container in ["orchestrator-test", "matcher-test", "embeddings-test", "extraction-test", "redis-test"]:
        run_docker_command(["stop", container])
    run_docker_command(["network", "rm", "test-network"])
    print("  Test infrastructure stopped")


@pytest.fixture(scope="session", autouse=True)
def test_infrastructure() -> Generator[None, None, None]:
    """Session fixture to start and stop test infrastructure."""
    start_test_infrastructure()
    yield
    stop_test_infrastructure()


class TestRedisStreamsIntegration:
    """Test Redis Streams message flow."""

    @pytest.fixture(autouse=True)
    def setup_redis(self):
        """Set up Redis connection for tests and ensure clean state."""
        import redis
        client = redis.Redis.from_url(REDIS_URL, decode_responses=True, db=1)
        client.flushdb()
        self.redis_client = client
        yield
        client.connection_pool.disconnect()

    def test_enqueue_message(self):
        """Test enqueueing a message to a stream."""
        stream = f"test:extraction:jobs:{uuid.uuid4().hex[:8]}"
        payload = {"task_id": "test-123", "data": "value"}

        msg_id = self.redis_client.xadd(stream, payload)
        assert msg_id is not None

        entries = self.redis_client.xrange(stream, min="-", max="+")
        assert len(entries) == 1
        assert entries[0][1]["task_id"] == "test-123"

    def test_consumer_group_creation(self):
        """Test consumer group is created automatically."""
        stream = f"test:extraction:jobs:{uuid.uuid4().hex[:8]}"

        msg_id = self.redis_client.xadd(stream, {"task_id": "test-456"})
        assert msg_id is not None

        info = self.redis_client.xinfo_stream(stream)
        assert info is not None
        assert info["length"] == 1

        self.redis_client.xgroup_create(stream, "test-consumer-group", id="0", mkstream=False)

        groups = self.redis_client.xinfo_groups(stream)
        group_names = [g["name"] for g in groups]
        assert "test-consumer-group" in group_names

    def test_ack_message(self):
        """Test acknowledging a message."""
        stream = f"test:extraction:jobs:{uuid.uuid4().hex[:8]}"

        self.redis_client.xadd(stream, {"task_id": "test-789"})

        self.redis_client.xgroup_create(stream, "test-consumer-group", id="0", mkstream=False)

        messages = self.redis_client.xreadgroup(
            "test-consumer-group",
            "test-consumer",
            {stream: ">"},
            count=1
        )
        assert messages is not None
        assert len(messages) > 0

        stream_messages = messages[0][1]
        assert len(stream_messages) > 0
        read_msg_id = stream_messages[0][0]

        result = self.redis_client.xack(stream, "test-consumer-group", read_msg_id)
        assert result == 1


class TestOrchestratorHealth:
    """Test orchestrator service health and connectivity."""

    def test_orchestrator_health_endpoint(self):
        """Test orchestrator health endpoint returns status."""
        response = requests.get(f"{ORCHESTRATOR_URL}/health", timeout=10)
        assert response.status_code == 200
        
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "orchestrator"
        assert "redis" in data
        assert data["redis"] == "connected"

    def test_all_microservices_health(self):
        """Test all microservices are healthy."""
        services = [
            ("Extraction", EXTRACTION_URL),
            ("Embeddings", EMBEDDINGS_URL),
            ("Scorer-Matcher", MATCHER_URL),
            ("Orchestrator", ORCHESTRATOR_URL),
        ]
        
        for name, url in services:
            response = requests.get(f"{url}/health", timeout=10)
            assert response.status_code == 200, f"{name} service is not healthy"
            
            data = response.json()
            assert data["status"] == "healthy", f"{name} service status is not healthy"


class TestOrchestratorPipeline:
    """Test orchestrator pipeline flow."""

    def test_publish_completion_event(self):
        """Test publishing completion event to pubsub."""
        from core.redis_streams import publish_completion

        channel = "test:extraction:completed"
        payload = {"task_id": "test-subscription", "status": "completed"}

        result = publish_completion(channel, payload)
        assert result >= 0

    def test_orchestrator_diagnostics_endpoint(self):
        """Test orchestrator diagnostics endpoint provides stream visibility."""
        response = requests.get(f"{ORCHESTRATOR_URL}/orchestrate/diagnostics")
        assert response.status_code == 200
        
        data = response.json()
        assert "success" in data
        assert "streams" in data

    def test_orchestrate_match_endpoint(self):
        """Test the orchestration match endpoint starts the pipeline."""
        # Trigger the pipeline, capturing the response stream to validate event delivery
        response = requests.post(f"{ORCHESTRATOR_URL}/orchestrate/match", json={}, stream=True)
        assert response.status_code == 200
        
        # Check if the endpoint responds with SSE (Server-Sent Events)
        if response.headers.get('content-type') == 'text/event-stream':
            first_event = next(response.iter_lines()).decode('utf-8')
            assert "data:" in first_event or "event:" in first_event
        else:
            # Fallback for standard JSON response
            data = response.json()
            assert data["success"] is True
            assert data["task_id"].startswith("match-")
            assert "Pipeline started" in data["message"]


class TestMicroservicesLogging:
    """Test that microservices produce expected log output."""

    def test_redis_streams_logs_enqueue_with_task_id(self, caplog):
        """Test that enqueue_job logs include task_id for traceability."""
        import os
        import logging

        # Use the test infrastructure Redis URL
        os.environ["REDIS_URL"] = REDIS_URL
        from core.redis_streams import enqueue_job
        
        stream = f"test:logs:jobs:{uuid.uuid4().hex[:8]}"
        task_id = f"test-{uuid.uuid4().hex[:8]}"

        with caplog.at_level(logging.INFO):
            _ = enqueue_job(stream, {"task_id": task_id})

            assert task_id in caplog.text
            assert "Enqueued" in caplog.text or "enqueued" in caplog.text

class TestPipelineErrorHandling:
    """Test error handling in pipeline to prevent silent failures."""

    def test_invalid_payload_returns_error(self):
        """Test that invalid payloads raise descriptive errors."""
        from core.redis_streams import enqueue_job
        
        with pytest.raises(ValueError, match="task_id"):
            enqueue_job("test:jobs", {"data": "no_task_id"})

    def test_non_serializable_payload_raises_error(self):
        """Test that non-JSON-serializable payloads raise descriptive errors."""
        from core.redis_streams import enqueue_job
        
        with pytest.raises(ValueError, match="serializable"):
            enqueue_job("test:jobs", {"task_id": "test-123", "data": lambda x: x})

    def test_completion_event_includes_status(self):
        """Test that completion events always include status field."""
        from core.redis_streams import publish_completion
        
        with pytest.raises(ValueError, match="status"):
            publish_completion("test:completed", {"task_id": "test-123"})