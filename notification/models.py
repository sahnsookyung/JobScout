from dataclasses import dataclass
from typing import Any, List


@dataclass
class NotificationDeliveryPlan:
    """Resolved notification settings for a matching run."""
    user_id: str
    enabled_channels: List[str]
    settings_snapshot: Any = None
