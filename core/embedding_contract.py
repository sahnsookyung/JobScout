"""Identity of the configured embedding space, independent of secrets and budgets."""

import hashlib
import json
from functools import lru_cache

from core.config_loader import LlmConfig, load_config

LEGACY_EMBEDDING_SPACE = "legacy"


def embedding_space_for_config(config: LlmConfig) -> str:
    if not config.embedding_input_type_enabled:
        return LEGACY_EMBEDDING_SPACE
    if config.embedding_dimensions != 1024:
        raise ValueError("The current database embedding contract requires 1024 dimensions")
    if not config.embedding_base_url or not config.embedding_model:
        raise ValueError("Asymmetric embeddings require an explicit endpoint and model")
    identity = {
        "endpoint": config.embedding_base_url.rstrip("/"),
        "model": config.embedding_model,
        "dimensions": config.embedding_dimensions,
        "roles": "query-passage-v1",
        "truncate": "NONE",
    }
    return "v1:" + hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()


@lru_cache(maxsize=1)
def current_embedding_space() -> str:
    """Workers use immutable startup configuration; restart after changing models."""
    return embedding_space_for_config(load_config().etl.llm)
