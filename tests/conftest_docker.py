"""
Test fixtures for Docker container management.

Provides pytest fixtures and utilities to spin up temporary PostgreSQL
and Redis containers for integration testing.

Usage:
    import pytest
    from tests.conftest_docker import postgres_container, redis_container
    
    def test_something(postgres_container, redis_container):
        # Use containers here
        pass
"""

import subprocess
import time
import socket
import os
import atexit
import uuid
from typing import Optional, Generator
from contextlib import contextmanager


class TestContainer:
    """Base class for managing Docker test containers."""
    
    def __init__(self, name: str, image: str, ports: dict, env: dict, 
                 ready_check: callable, ready_timeout: int = 30):
        self.name = name
        self.image = image
        self.ports = ports
        self.env = env
        self.ready_check = ready_check
        self.ready_timeout = ready_timeout
        self.container_id: Optional[str] = None
        self.is_running = False
        
    def start(self) -> bool:
        """Start the container and wait for it to be ready."""
        try:
            # Check if Docker is available
            subprocess.run(['docker', 'version'], capture_output=True, check=True)
            
            # Generate unique container name
            unique_name = f"{self.name}-{uuid.uuid4().hex[:8]}"
            
            # Build port mappings
            port_args = []
            for host_port, container_port in self.ports.items():
                port_args.extend(['-p', f'{host_port}:{container_port}'])
            
            # Build env args
            env_args = []
            for key, value in self.env.items():
                env_args.extend(['-e', f'{key}={value}'])
            
            # Run container
            cmd = ['docker', 'run', '-d', '--name', unique_name] + port_args + env_args + [self.image]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"Failed to start container: {result.stderr}")
                return False
            
            self.container_id = result.stdout.strip()
            self.name = unique_name
            
            # Wait for container to be ready
            start_time = time.time()
            while time.time() - start_time < self.ready_timeout:
                if self.ready_check():
                    self.is_running = True
                    # Register cleanup on exit
                    atexit.register(self.stop)
                    return True
                time.sleep(0.5)
            
            # Timeout - stop container
            self.stop()
            return False
            
        except FileNotFoundError:
            print("Docker not available")
            return False
        except Exception as e:
            print(f"Error starting container: {e}")
            self.stop()
            return False
    
    def stop(self):
        """Stop and remove the container."""
        if self.container_id:
            try:
                subprocess.run(['docker', 'stop', self.container_id], 
                             capture_output=True, timeout=10)
                subprocess.run(['docker', 'rm', '-v', self.container_id], 
                             capture_output=True, timeout=10)
            except Exception as e:
                print(f"Error stopping container: {e}")
            finally:
                self.is_running = False
                self.container_id = None


class PostgresContainer(TestContainer):
    """PostgreSQL with pgvector container for testing."""
    
    def __init__(self, host_port: int = 15432):
        super().__init__(
            name='jobscout-test-postgres',
            image='ankane/pgvector:latest',
            ports={host_port: 5432},
            env={
                'POSTGRES_USER': 'test',
                'POSTGRES_PASSWORD': 'test',
                'POSTGRES_DB': 'jobscout_test'
            },
            ready_check=lambda: self._check_ready(host_port),
            ready_timeout=30
        )
        self.host_port = host_port
        self.database_url = f"postgresql://test:test@localhost:{host_port}/jobscout_test"
    
    def _check_ready(self, port: int) -> bool:
        """Check if PostgreSQL is accepting connections."""
        try:
            import psycopg2
            conn = psycopg2.connect(
                host='localhost',
                port=port,
                user='test',
                password='test',
                database='jobscout_test',
                connect_timeout=2
            )
            conn.close()
            return True
        except:
            return False


class RedisContainer(TestContainer):
    """Redis container for testing."""
    
    def __init__(self, host_port: int = 16379):
        super().__init__(
            name='jobscout-test-redis',
            image='redis:7-alpine',
            ports={host_port: 6379},
            env={},
            ready_check=lambda: self._check_ready(host_port),
            ready_timeout=10
        )
        self.host_port = host_port
        self.redis_url = f"redis://localhost:{host_port}/0"
    
    def _check_ready(self, port: int) -> bool:
        """Check if Redis is accepting connections."""
        try:
            import redis
            r = redis.Redis(host='localhost', port=port, socket_connect_timeout=2)
            return r.ping()
        except:
            return False


@contextmanager
def postgres_container(host_port: int = 15432) -> Generator[PostgresContainer, None, None]:
    """
    Context manager for PostgreSQL test container.
    
    Usage:
        with postgres_container() as pg:
            # Use pg.database_url
            pass
    """
    container = PostgresContainer(host_port)
    if not container.start():
        raise RuntimeError("Failed to start PostgreSQL container")
    try:
        yield container
    finally:
        container.stop()


@contextmanager
def redis_container(host_port: int = 16379) -> Generator[RedisContainer, None, None]:
    """
    Context manager for Redis test container.
    
    Usage:
        with redis_container() as redis:
            # Use redis.redis_url
            pass
    """
    container = RedisContainer(host_port)
    if not container.start():
        raise RuntimeError("Failed to start Redis container")
    try:
        yield container
    finally:
        container.stop()


# Pytest fixtures (optional - for pytest-based tests)
try:
    import pytest
    
    @pytest.fixture(scope='session')
    def docker_postgres():
        """Pytest fixture for PostgreSQL container."""
        with postgres_container() as pg:
            yield pg
    
    @pytest.fixture(scope='session')
    def docker_redis():
        """Pytest fixture for Redis container."""
        with redis_container() as redis:
            yield redis

except ImportError:
    pass  # pytest not installed
