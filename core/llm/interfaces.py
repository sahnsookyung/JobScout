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
    def extract_structured_data(self, text: str, schema_spec: Dict) -> Dict[str, Any]:
        """
        Extract structured JSON data from text adhering to a schema.

        Args:
            text: Text to extract from
            schema_spec: Either a wrapped spec {'name', 'strict', 'schema'} or raw JSON schema
        """
        pass

    @abstractmethod
    def generate_embedding(self, text: str) -> List[float]:
        """
        Generate a vector embedding for the given text.
        """
        pass
    
    @abstractmethod
    def extract_job_facets(self, text: str) -> Dict[str, str]:
        """
        Extract per-facet text from job description for Want score matching.
        
        Returns a dictionary with keys:
        - remote_flexibility: Text about remote work, WFH policies
        - compensation: Text about salary, bonuses, equity, benefits
        - learning_growth: Text about learning opportunities, mentorship
        - company_culture: Text about company values, DEI, work environment
        - work_life_balance: Text about working hours, PTO, burnout prevention
        - tech_stack: Text about technologies, tools, frameworks used
        - visa_sponsorship: Text about visa and relocation details
        """
        pass
