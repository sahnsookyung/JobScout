"""
OpenAI Service - LLM implementation using OpenAI API.

Provides structured data extraction and embedding generation.
"""
from typing import Dict, Any, List, Optional, Tuple
import json
import logging
import copy
from openai import OpenAI
from core.llm.interfaces import LLMProvider
from etl.schema_models import FACET_EXTRACTION_SCHEMA_FOR_WANTS

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
        model_config: Optional[Dict[str, Any]] = None
    ):
        client_kwargs = {}
        if api_key:
            client_kwargs['api_key'] = api_key
        if base_url:
            client_kwargs['base_url'] = base_url
            
        self.client = OpenAI(**client_kwargs)
        self.model_config = model_config or {}
        self.extraction_model = self.model_config.get('extraction_model', 'qwen3:14b')
        self.embedding_model = self.model_config.get('embedding_model', 'qwen3-embedding:4b')
        self.embedding_dimensions = self.model_config.get('embedding_dimensions', 1024)

    def extract_structured_data(self, text: str, schema_spec: Dict) -> Dict[str, Any]:
        """Extract structured data using JSON Schema mode.

        Args:
            text: Text to extract from
            schema_spec: Either a wrapped spec {'name', 'strict', 'schema'} or raw JSON schema
        """
        name, strict, raw_schema = _unwrap_schema_spec(schema_spec)
        runtime_schema = copy.deepcopy(raw_schema)

        if runtime_schema.get("type") != "object" or "properties" not in runtime_schema:
            raise ValueError(f"Not a valid JSON Schema object. Top-level keys: {list(runtime_schema.keys())}")

        messages = [
            {"role": "system", "content": "You are a helpful assistant that extracts structured data from job descriptions."},
            {"role": "user", "content": f"Extract the data into the requested JSON format.\n\nDescription:\n{text}"},
        ]

        response = self.client.chat.completions.create(
            model=self.extraction_model,
            messages=messages,
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": name,
                    "schema": runtime_schema,
                    "strict": strict,
                },
            },
        )

        content = response.choices[0].message.content
        data = json.loads(content)

        thought_process = data.get('thought_process', 'No reasoning provided.')
        logger.info("=" * 60)
        logger.info(f"MODEL THINKING ({self.extraction_model}):")
        logger.info("-" * 60)
        logger.info(thought_process)
        logger.info("-" * 60)

        return data

    def generate_embedding(self, text: str) -> List[float]:
        """Generate embedding vector for text."""
        try:
            response = self.client.embeddings.create(
                input=text,
                model=self.embedding_model,
                dimensions=self.embedding_dimensions
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Embedding failed: {e}")
            raise
    
    def extract_job_facets(self, text: str) -> Dict[str, str]:
        """Extract per-facet text from job description using FACET_EXTRACTION_SCHEMA_FOR_WANTS.

        Returns a dictionary with keys:
        - remote_flexibility
        - compensation
        - learning_growth
        - company_culture
        - work_life_balance
        - tech_stack
        - visa_sponsorship
        """
        name, strict, raw_schema = _unwrap_schema_spec(FACET_EXTRACTION_SCHEMA_FOR_WANTS)

        if raw_schema.get("type") != "object" or "properties" not in raw_schema:
            raise ValueError(f"Not a valid JSON Schema object. Top-level keys: {list(raw_schema.keys())}")

        try:
            response = self.client.chat.completions.create(
                model=self.extraction_model,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that extracts job facet information from descriptions."},
                    {"role": "user", "content": f"Extract facet information from the job description below.\n\nDescription:\n{text}"}
                ],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": name,
                        "schema": raw_schema,
                        "strict": strict,
                    }
                }
            )

            content = response.choices[0].message.content
            data = json.loads(content)

            logger.debug(f"Extracted facets: {list(data.keys())}")

            return data

        except Exception as e:
            logger.error(f"Facet extraction failed: {e}")
            raise

    def unload_model(self, model_name: str):
        """Unload model from Ollama (no-op for pure OpenAI)."""
        if "localhost" in str(self.client.base_url) or "127.0.0.1" in str(self.client.base_url) or "host.docker.internal" in str(self.client.base_url):
            import requests
            try:
                base = str(self.client.base_url).rstrip('/').replace('/v1', '')
                url = f"{base}/api/generate"
                payload = {"model": model_name, "keep_alive": 0}
                logger.info(f"Unloading model: {model_name} via {url}")
                requests.post(url, json=payload)
            except Exception as e:
                logger.warning(f"Failed to unload model {model_name}: {e}")
