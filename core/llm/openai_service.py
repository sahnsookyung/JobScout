"""
OpenAI Service - LLM implementation using OpenAI API.

Provides structured data extraction and embedding generation.
"""
from typing import Dict, Any, List, Optional, Tuple, Literal
import json
import logging
import copy
import requests

from openai import OpenAI
from core.llm.interfaces import LLMProvider
from core.llm.system_prompts import (
    DEFAULT_EXTRACTION_SYSTEM_PROMPT,
    RESUME_EXTRACTION_SYSTEM_PROMPT,
    REQUIREMENTS_EXTRACTION_SYSTEM_PROMPT,
    FACET_EXTRACTION_SYSTEM_PROMPT,
)
from core.llm.schema_models import (
    RESUME_SCHEMA,
    EXTRACTION_SCHEMA,
    FACET_EXTRACTION_SCHEMA_FOR_WANTS,
)

logger = logging.getLogger(__name__)

def _unwrap_schema_spec(spec: Dict[str, Any]) -> Tuple[str, bool, Dict[str, Any]]:
    """Unwrap a schema spec to extract name, strict flag, and raw JSON schema.

    Args:
        spec: Either a wrapped spec {'name': str, 'strict': bool, 'schema': {...}}
              or a raw JSON schema dict

    Returns:
        Tuple of (name, strict, raw_schema)
    """
    if isinstance(spec, dict) and "schema" in spec and "name" in spec:
        return spec.get("name", "extraction_response"), bool(spec.get("strict", False)), spec["schema"]
    return "extraction_response", False, spec


class OpenAIService(LLMProvider):
    """
    OpenAI LLM Service.
    
    Provides structured data extraction using JSON Schema mode
    and embedding generation.
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model_config: Optional[Dict[str, Any]] = None,
        embedding_api_key: Optional[str] = None,
        embedding_base_url: Optional[str] = None
    ):
        client_kwargs = {}
        if api_key:
            client_kwargs['api_key'] = api_key
        if base_url:
            client_kwargs['base_url'] = base_url
            
        self.client = OpenAI(**client_kwargs)
        
        if embedding_base_url or embedding_api_key:
            embedding_client_kwargs = {}
            if embedding_api_key:
                embedding_client_kwargs['api_key'] = embedding_api_key
            if embedding_base_url:
                embedding_client_kwargs['base_url'] = embedding_base_url
            self.embedding_client = OpenAI(**embedding_client_kwargs)
        else:
            self.embedding_client = None
        
        self.model_config = model_config or {}
        self.extraction_model = self.model_config.get('extraction_model', 'qwen3:14b')
        self.embedding_model = self.model_config.get('embedding_model', 'qwen3-embedding:4b')
        self.embedding_dimensions = self.model_config.get('embedding_dimensions', 1024)
        self.extraction_temperature = self.model_config.get('extraction_temperature', 0.0)

    def extract_structured_data(
        self,
        text: str,
        schema_spec: Dict,
        system_prompt: Optional[str] = None,
        user_message: Optional[str] = None
    ) -> Dict[str, Any]:
        """Extract structured data using JSON Schema mode.

        Args:
            text: Text to extract from
            schema_spec: Either a wrapped spec {'name', 'strict', 'schema'} or raw JSON schema
            system_prompt: Optional custom system prompt. If None, uses default.
            user_message: Optional custom user message. If None, uses default.
        """
        name, strict, raw_schema = _unwrap_schema_spec(schema_spec)
        runtime_schema = copy.deepcopy(raw_schema)

        if runtime_schema.get("type") != "object" or "properties" not in runtime_schema:
            raise ValueError(f"Not a valid JSON Schema object. Top-level keys: {list(runtime_schema.keys())}")

        if system_prompt is None:
            system_prompt = DEFAULT_EXTRACTION_SYSTEM_PROMPT

        if user_message is None:
            user_message = f"Extract the data into the requested JSON format.\n\nDescription:\n{text}"

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ]

        response = self.client.chat.completions.create(
            model=self.extraction_model,
            messages=messages,
            temperature=self.extraction_temperature,
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": name,
                    "schema": runtime_schema,
                    "strict": strict,
                },
            },
        )

        try:
            content = response.choices[0].message.content
            data = json.loads(content)
        except (json.JSONDecodeError, IndexError, AttributeError) as e:
            logger.error(f"Failed to parse structured data response: {e}")
            raise

        thought_process = data.get('thought_process', 'No reasoning provided.')
        logger.info("=" * 60)
        logger.info(f"MODEL THINKING ({self.extraction_model}):")
        logger.info("-" * 60)
        logger.info(thought_process)
        logger.info("=" * 60)

        return data

    def extract_resume_data(self, text: str) -> Dict[str, Any]:
        """Extract structured data from resumes using specialized resume instructions.

        Args:
            text: Resume text to extract from

        Returns:
            Extracted resume data following the RESUME_SCHEMA
        """
        data = self.extract_structured_data(
            text,
            RESUME_SCHEMA,
            system_prompt=RESUME_EXTRACTION_SYSTEM_PROMPT,
            user_message=f"Extract the structured resume data following the schema.\n\nResume:\n{text}"
        )

        logger.info("=" * 60)
        logger.info(f"RESUME EXTRACTION ({self.extraction_model}):")
        logger.info("-" * 60)
        logger.info(f"Extracted profile with {len(data.get('profile', {}).get('experience', []))} experience entries")
        logger.info("=" * 60)

        return data

    def extract_requirements_data(self, text: str) -> Dict[str, Any]:
        """Extract structured qualification requirements from job descriptions.

        Args:
            text: Job description text

        Returns:
            Extracted requirements with required/preferred classifications
        """
        data = self.extract_structured_data(
            text,
            EXTRACTION_SCHEMA,
            system_prompt=REQUIREMENTS_EXTRACTION_SYSTEM_PROMPT,
            user_message=f"<JOB_DESCRIPTION>\n{text}\n</JOB_DESCRIPTION>\n\nExtract all qualification requirements."
        )

        logger.info("=" * 60)
        logger.info(f"REQUIREMENTS EXTRACTION ({self.extraction_model}):")
        logger.info("-" * 60)
        logger.info("extract_structured_data returned type=%s value=%r", type(data), data)
        logger.info("=" * 60)

        return data

    def extract_facet_data(self, text: str) -> Dict[str, str]:
        """Extract per-facet text from job description for Want score matching.

        Args:
            text: Job description text

        Returns:
            Dictionary with keys:
            - remote_flexibility
            - compensation
            - learning_growth
            - company_culture
            - work_life_balance
            - tech_stack
            - visa_sponsorship
        """
        data = self.extract_structured_data(
            text,
            FACET_EXTRACTION_SCHEMA_FOR_WANTS,
            system_prompt=FACET_EXTRACTION_SYSTEM_PROMPT,
            user_message=f"<JOB_DESCRIPTION>\n{text}\n</JOB_DESCRIPTION>\n\nExtract all 7 facets from this job description."
        )

        logger.debug(f"Extracted facets: {list(data.keys())}")
        return data

    def generate_embedding(self, text: str) -> List[float]:
        """Generate embedding vector for text."""
        try:
            client = self.embedding_client if self.embedding_client else self.client
            response = client.embeddings.create(
                input=text,
                model=self.embedding_model,
                dimensions=self.embedding_dimensions
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Embedding failed: {e}")
            raise

    def unload_model(self, model_name: str):
        """Unload model from Ollama (no-op for pure OpenAI)."""
        if "localhost" in str(self.client.base_url) or "127.0.0.1" in str(self.client.base_url) or "host.docker.internal" in str(self.client.base_url):
            try:
                base = str(self.client.base_url).rstrip('/').replace('/v1', '')
                url = f"{base}/api/generate"
                payload = {"model": model_name, "keep_alive": 0}
                logger.info(f"Unloading model: {model_name} via {url}")
                requests.post(url, json=payload)
            except Exception as e:
                logger.warning(f"Failed to unload model {model_name}: {e}")
