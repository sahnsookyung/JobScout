from dataclasses import dataclass
from typing import Optional

from core.config_loader import AppConfig, LlmConfig
from core.llm.openai_service import OpenAIService
from core.scraper.jobspy_client import JobSpyClient
from etl.orchestrator import JobETLService
from notification.service import NotificationService


@dataclass
class AppContext:
    """Application context container that holds all wired dependencies.

    This eliminates duplicate wiring code and provides a single source
    of truth for service instantiation. DB access should be obtained
    via job_uow() inside each processing loop.
    """
    config: AppConfig
    ai_service: OpenAIService
    job_etl_service: JobETLService
    jobspy_client: JobSpyClient
    notification_service: Optional[NotificationService] = None

    @classmethod
    def build(cls, config: AppConfig) -> "AppContext":
        """Build an AppContext from config.

        Args:
            config: Loaded application configuration

        Returns:
            Fully wired AppContext instance (no DB session attached)
        """
        # AI Service
        llm_config = config.etl.llm if (config.etl and config.etl.llm) else LlmConfig()
        ai_service = cls._build_ai_service(llm_config)

        # ETL Service (does not hold repo - repo passed per-operation)
        job_etl_service = JobETLService(ai_service)

        # JobSpy Client
        jobspy_client = cls._build_jobspy_client(config)

        # Notification Service (lazy - only if enabled)
        notification_service = None
        if config.notifications and config.notifications.enabled:
            notification_service = cls._build_notification_service(config)

        return cls(
            config=config,
            ai_service=ai_service,
            job_etl_service=job_etl_service,
            jobspy_client=jobspy_client,
            notification_service=notification_service
        )

    @staticmethod
    def _build_ai_service(llm_config: LlmConfig) -> OpenAIService:
        """Build OpenAI service from LLM configuration."""
        model_config = {
            'extraction_model': llm_config.extraction_model,
            'embedding_model': llm_config.embedding_model,
            'embedding_dimensions': llm_config.embedding_dimensions,
            'extraction_temperature': llm_config.extraction_temperature,
        }

        return OpenAIService(
            base_url=llm_config.base_url,
            api_key=llm_config.api_key,
            api_secret=llm_config.api_secret,
            model_config=model_config,
            extraction_headers=llm_config.extraction_headers,
            embedding_base_url=llm_config.embedding_base_url,
            embedding_api_key=llm_config.embedding_api_key,
            embedding_api_secret=llm_config.embedding_api_secret,
            embedding_headers=llm_config.embedding_headers
        )

    @staticmethod
    def _build_jobspy_client(config: AppConfig) -> JobSpyClient:
        """Build JobSpy client from configuration."""
        jobspy_config = config.jobspy

        # Get base URL from config (no hard-coding)
        base_url = jobspy_config.url if jobspy_config else None

        # Get timeouts from config with sensible defaults
        poll_interval = getattr(jobspy_config, 'poll_interval_seconds', 10)
        job_timeout = getattr(jobspy_config, 'job_timeout_seconds', 300)
        request_timeout = getattr(jobspy_config, 'request_timeout_seconds', 30)

        return JobSpyClient(
            base_url=base_url,
            poll_interval_seconds=poll_interval,
            job_timeout_seconds=job_timeout,
            request_timeout_seconds=request_timeout
        )

    @staticmethod
    def _build_notification_service(
        config: AppConfig
    ) -> Optional[NotificationService]:
        """Build notification service if enabled in config.

        Creates a temporary session for the notification service repository.
        The service manages its own session lifecycle internally.
        """
        notification_config = config.notifications

        if not notification_config or not notification_config.enabled:
            return None

        from database.database import SessionLocal
        from database.repository import JobRepository

        # Create a session for the notification service
        # Note: The service uses this repo for tracker initialization,
        # but should create fresh sessions for actual DB operations
        session = SessionLocal()
        try:
            repo = JobRepository(session)
            return NotificationService(
                repo=repo,
                redis_url=notification_config.redis_url,
                base_url=notification_config.base_url,
                use_async_queue=notification_config.use_async_queue
            )
        except Exception:
            session.close()
            raise
