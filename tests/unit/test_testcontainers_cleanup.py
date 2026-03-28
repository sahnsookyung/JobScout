import os

import pytest
import sqlalchemy
import testcontainers.postgres as postgres_module
import testcontainers.redis as redis_module

import database.migrate as migrate_module
import tests.conftest as project_conftest


class _FakeEngine:
    def __init__(self) -> None:
        self.disposed = False

    def dispose(self) -> None:
        self.disposed = True


def test_test_database_stops_container_when_setup_fails(monkeypatch) -> None:
    class FakePostgresContainer:
        def __init__(self, *args, **kwargs) -> None:
            self.stopped = False

        def start(self) -> None:
            return None

        def get_connection_url(self) -> str:
            return "postgresql://testuser:testpass@localhost:5432/jobscout_test"

        def stop(self) -> None:
            self.stopped = True

    container = FakePostgresContainer()
    engine = _FakeEngine()

    def fail_migrate(*, engine) -> None:
        raise RuntimeError("migration failed")

    monkeypatch.delenv("TEST_DATABASE_URL", raising=False)
    monkeypatch.setattr(postgres_module, "PostgresContainer", lambda *args, **kwargs: container)
    monkeypatch.setattr(sqlalchemy, "create_engine", lambda url: engine)
    monkeypatch.setattr(migrate_module, "migrate_database", fail_migrate)

    fixture = project_conftest.test_database.__wrapped__
    generator = fixture()

    with pytest.raises(pytest.skip.Exception):
        next(generator)

    assert container.stopped is True
    assert engine.disposed is True
    assert "TEST_DATABASE_URL" not in os.environ


def test_redis_container_stops_container_when_setup_fails(monkeypatch) -> None:
    class FakeRedisContainer:
        def __init__(self, image: str) -> None:
            self.stopped = False

        def start(self) -> None:
            return None

        def get_container_host_ip(self) -> str:
            return "127.0.0.1"

        def get_exposed_port(self, port: int) -> str:
            raise RuntimeError("port inspection failed")

        def stop(self) -> None:
            self.stopped = True

    container = FakeRedisContainer("redis:7-alpine")

    monkeypatch.delenv("TEST_REDIS_URL", raising=False)
    monkeypatch.setattr(redis_module, "RedisContainer", lambda image: container)

    fixture = project_conftest.redis_container.__wrapped__
    generator = fixture()

    with pytest.raises(pytest.skip.Exception):
        next(generator)

    assert container.stopped is True
    assert "TEST_REDIS_URL" not in os.environ
