"""
LLM Provider Interface - Abstract base for AI service providers.

This module defines the interface for LLM services (OpenAI, Ollama, Anthropic, etc.).
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Any


class LLMProvider(ABC):
    """
    Abstract Interface for AI Service Providers (OpenAI, Ollama, Anthropic, etc.).
    """
    
    @abstractmethod
    def extract_structured_data(self, text: str, schema: Dict) -> Dict[str, Any]:
        """
        Extract structured JSON data from text adhering to a schema.
        """
        pass

    @abstractmethod
    def generate_embedding(self, text: str) -> List[float]:
        """
        Generate a vector embedding for the given text.
        """
        pass
