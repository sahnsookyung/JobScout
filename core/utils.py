import hashlib
import logging
from typing import Any

logger = logging.getLogger(__name__)


def cosine_similarity_from_distance(distance: float) -> float:
    """Convert pgvector cosine distance to cosine similarity, clipped to [0, 1].

    pgvector cosine_distance returns values in range [0, 2], so similarity
    can theoretically be in range [-1, 1]. In practice with normalized
    embeddings, distance should be [0, 1] and similarity should be [0, 1].

    Args:
        distance: Cosine distance from pgvector

    Returns:
        Cosine similarity in range [0, 1]
    """
    similarity = 1.0 - float(distance)
    if not (0.0 <= similarity <= 1.0):
        logger.error(f"Similarity out of range: {similarity}, clipping to [0, 1]")
        return max(0.0, min(1.0, similarity))
    return similarity

class JobFingerprinter:
    """
    Pure logic for creating deterministic fingerprints for deduplication.
    """
    
    @staticmethod
    def calculate(company: str, title: str, location_text: str) -> str:
        """
        Create a deterministic hash of the core immutable fields.
        Formula: SHA256(lowercase(Company) + lowercase(JobTitle) + lowercase(City/Location))
        """
        raw_string = f"{company.lower().strip()}|{title.lower().strip()}|{location_text.lower().strip()}"
        return hashlib.sha256(raw_string.encode('utf-8')).hexdigest()

    @staticmethod
    def normalize_location(location: Any) -> str:
        """
        Normalize location data which can be a dict, string, or list.
        """
        location_text = "Unknown"
        if isinstance(location, dict):
            location_text = location.get('city') or location.get('country') or "Unknown"
            if isinstance(location_text, list): # Handle ["japan", "jp"]
                location_text = location_text[0]
        elif isinstance(location, str):
            location_text = location
        return str(location_text)
