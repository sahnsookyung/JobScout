"""Application Context Container - SOLID-lite dependency injection."""

from dataclasses import dataclass
from typing import Optional

from sqlalchemy.orm import Session

from core.config_loader import AppConfig, LlmConfig
from core.ai_service import OpenAIService
from core.jobspy_client import JobSpyClient
from database.repository import JobRepository
from etl.orchestrator import JobETLOrchestrator
from notification.service import NotificationService


@dataclass
class AppContext:
    """
    Application context container that holds all wired dependencies.
    
    This eliminates duplicate wiring code and provides a single source
    of truth for service instantiation.
    """
    config: AppConfig
    session: Session
    repo: JobRepository
    ai_service: OpenAIService
    orchestrator: JobETLOrchestrator
    jobspy_client: JobSpyClient
    notification_service: Optional[NotificationService] = None
    
    @classmethod
    def build(cls, config: AppConfig, session: Session) -> "AppContext":
        """
        Build an AppContext from config and database session.
        
        Args:
            config: Loaded application configuration
            session: SQLAlchemy database session
            
        Returns:
            Fully wired AppContext instance
        """
        # Repository
        repo = JobRepository(session)
        
        # AI Service
        llm_config = config.etl.llm if (config.etl and config.etl.llm) else LlmConfig()
        ai_service = cls._build_ai_service(llm_config)
        
        # ETL Orchestrator
        orchestrator = JobETLOrchestrator(repo, ai_service)
        
        # JobSpy Client
        jobspy_client = cls._build_jobspy_client(config)
        
        # Notification Service (lazy - only if enabled)
        notification_service = None
        if config.notifications and config.notifications.enabled:
            notification_service = cls._build_notification_service(config, repo)
        
        return cls(
            config=config,
            session=session,
            repo=repo,
            ai_service=ai_service,
            orchestrator=orchestrator,
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
        }
        
        return OpenAIService(
            base_url=llm_config.base_url,
            api_key=llm_config.api_key,
            model_config=model_config
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
        config: AppConfig, 
        repo: JobRepository
    ) -> Optional[NotificationService]:
        """Build notification service if enabled in config."""
        notification_config = config.notifications
        
        if not notification_config or not notification_config.enabled:
            return None
        
        return NotificationService(
            repo=repo,
            redis_url=notification_config.redis_url,
            base_url=notification_config.base_url,
            use_async_queue=notification_config.use_async_queue
        )
