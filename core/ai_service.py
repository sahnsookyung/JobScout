from typing import Dict, Any, List, Optional
import json
import logging
import copy
from openai import OpenAI
from core.interfaces import LLMProvider

logger = logging.getLogger(__name__)

class OpenAIService(LLMProvider):
    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None, model_config: Optional[Dict[str, Any]] = None):
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

    def extract_structured_data(self, text: str, base_schema: Dict) -> Dict[str, Any]:
        """
        Extracts structured data using the OpenAI JSON Schema mode.
        Injects a 'thought_process' field to encourage Chain-of-thought reasoning independently of the DB schema.
        """
        try:
            # 1. DEEP COPY the schema
            runtime_schema = copy.deepcopy(base_schema)

            # 2. Inject 'thought_process' for CoT
            runtime_schema["properties"]["thought_process"] = {
                "type": "string",
                "description": "Step-by-step reasoning. Identify the tech stack, hard requirements vs nice-to-haves, and any ambiguities found in the text."
            }

            # 3. Force 'thought_process' first
            if "required" not in runtime_schema:
                runtime_schema["required"] = []
            if "thought_process" not in runtime_schema["required"]:
                runtime_schema["required"].insert(0, "thought_process")

            messages = [
                {"role": "system", "content": "You are a helpful assistant that extracts structured data from job descriptions."},
                {"role": "user", "content": f"Analyze the job description below. First, write your reasoning in the 'thought_process' field. Then, extract the job requirements into the requested JSON format.\n\nDescription:\n{text}"}
            ]

            response = self.client.chat.completions.create(
                model=self.extraction_model,
                messages=messages,
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "extraction_response",
                        "schema": runtime_schema,
                        "strict": False
                    }
                }
            )
            
            content = response.choices[0].message.content
            data = json.loads(content)
            
            # Log thinking process
            thought_process = data.get('thought_process', 'No reasoning provided.')
            logger.info("=" * 60)
            logger.info(f"🧠 MODEL THINKING ({self.extraction_model}):")
            logger.info("-" * 60)
            logger.info(thought_process)
            logger.info("-" * 60)
            
            return data

        except Exception as e:
            logger.error(f"LLM extraction failed: {e}")
            raise

    def generate_embedding(self, text: str) -> List[float]:
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

    def unload_model(self, model_name: str):
        # Specific to Ollama; for pure OpenAI this is a no-op
        if "localhost" in str(self.client.base_url) or "127.0.0.1" in str(self.client.base_url) or "host.docker.internal" in str(self.client.base_url):
             import requests
             try:
                # Construct API URL from base_url (e.g. http://localhost:11434/v1 -> http://localhost:11434/api/generate)
                base = str(self.client.base_url).rstrip('/').replace('/v1', '')
                url = f"{base}/api/generate"
                payload = {"model": model_name, "keep_alive": 0}
                logger.info(f"Unloading model: {model_name} via {url}")
                requests.post(url, json=payload)
             except Exception as e:
                 logger.warning(f"Failed to unload model {model_name}: {e}")
