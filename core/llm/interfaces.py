"""
LLM Provider Interface - Abstract base for AI service providers.

This module defines the interface for LLM services (OpenAI, Ollama, Anthropic, etc.).
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional


class LLMProvider(ABC):
    """
    Abstract Interface for AI Service Providers (OpenAI, Ollama, Anthropic, etc.).
    """

    @abstractmethod
    def extract_structured_data(
        self,
        text: str,
        schema_spec: Dict,
        system_prompt: Optional[str] = None,
        user_message: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Extract structured JSON data from text adhering to a schema.

        Args:
            text: Text to extract from
            schema_spec: Either a wrapped spec {'name', 'strict', 'schema'} or raw JSON schema
            system_prompt: Optional custom system prompt. If None, uses default.
            user_message: Optional custom user message. If None, uses default.
        """
        pass

    @abstractmethod
    def extract_resume_data(self, text: str) -> Dict[str, Any]:
        """
        Extract structured data from resumes using specialized instructions.

        Args:
            text: Resume text to extract from
        """
        pass

    @abstractmethod
    def extract_requirements_data(self, text: str) -> Dict[str, Any]:
        """
        Extract structured qualification requirements from job descriptions.

        Args:
            text: Job description text
        """
        pass

    @abstractmethod
    def generate_embedding(self, text: str) -> List[float]:
        """
        Generate a vector embedding for the given text.
        """
        pass

    def generate_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple texts.

        Default implementation calls generate_embedding in a loop.
        Override for providers that support native batch embedding APIs.
        """
        return [self.generate_embedding(text) for text in texts]
