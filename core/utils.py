import hashlib
from typing import Any

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
