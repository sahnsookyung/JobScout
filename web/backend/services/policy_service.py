#!/usr/bin/env python3
"""
Policy service - manages result filtering policies.
"""

import json
import logging
from typing import Optional, Dict
from dataclasses import dataclass

from database.database import db_session_scope
from ..exceptions import InvalidPolicyException

logger = logging.getLogger(__name__)


@dataclass
class ResultPolicy:
    """Result policy configuration."""
    min_fit: float = 55.0
    top_k: int = 50
    min_jd_required_coverage: Optional[float] = 0.6


# Policy presets
POLICY_PRESETS: Dict[str, ResultPolicy] = {
    "strict": ResultPolicy(min_fit=70.0, min_jd_required_coverage=0.80, top_k=25),
    "balanced": ResultPolicy(min_fit=55.0, min_jd_required_coverage=0.60, top_k=50),
    "discovery": ResultPolicy(min_fit=40.0, min_jd_required_coverage=None, top_k=100),
}


class PolicyService:
    """Service for managing result policies."""
    
    def __init__(self):
        self._default_policy = POLICY_PRESETS["balanced"]
        self._current_policy = self._load_from_db()
    
    def get_current_policy(self) -> ResultPolicy:
        """
        Get the current result policy.
        
        Returns:
            ResultPolicy: The active policy.
        """
        return self._current_policy
    
    def update_policy(
        self,
        min_fit: float,
        top_k: int,
        min_jd_required_coverage: Optional[float]
    ) -> ResultPolicy:
        """
        Update the result policy.
        
        Args:
            min_fit: Minimum fit score (0-100).
            top_k: Maximum results to return (1-500).
            min_jd_required_coverage: Minimum coverage (0-1) or None.
        
        Returns:
            Updated policy.
        
        Raises:
            InvalidPolicyException: If policy values are invalid.
            Exception: If DB save fails.
        """
        if not (0 <= min_fit <= 100):
            raise InvalidPolicyException(
                f"min_fit must be between 0 and 100, got {min_fit}"
            )
        
        if not (1 <= top_k <= 500):
            raise InvalidPolicyException(
                f"top_k must be between 1 and 500, got {top_k}"
            )
        
        if min_jd_required_coverage is not None and not (0.0 <= min_jd_required_coverage <= 1.0):
            raise InvalidPolicyException(
                f"min_jd_required_coverage must be between 0.0 and 1.0, got {min_jd_required_coverage}"
            )
        
        new_policy = ResultPolicy(
            min_fit=min_fit,
            top_k=top_k,
            min_jd_required_coverage=min_jd_required_coverage
        )
        
        self._save_to_db(new_policy)
        self._current_policy = new_policy
        
        return new_policy
    
    def apply_preset(self, preset_name: str) -> ResultPolicy:
        """
        Apply a policy preset.
        
        Args:
            preset_name: Name of the preset (strict, balanced, discovery).
        
        Returns:
            Applied policy.
        
        Raises:
            InvalidPolicyException: If preset is not found.
        """
        preset_name = preset_name.lower()
        
        if preset_name not in POLICY_PRESETS:
            raise InvalidPolicyException(
                f"Invalid preset '{preset_name}'. "
                f"Valid options: {', '.join(POLICY_PRESETS.keys())}"
            )
        
        policy = POLICY_PRESETS[preset_name]
        self._save_to_db(policy)
        self._current_policy = policy
        
        return policy
    
    def get_presets(self) -> Dict[str, ResultPolicy]:
        """Get all available policy presets."""
        return POLICY_PRESETS.copy()
    
    # Private methods
    
    def _load_from_db(self) -> ResultPolicy:
        """Load policy from database or return default."""
        try:
            from database.models import AppSettings
            
            with db_session_scope() as session:
                setting = session.query(AppSettings).filter(
                    AppSettings.key == 'result_policy'
                ).first()
                
                if setting and setting.value:
                    data = self._parse_value(setting.value)
                    return ResultPolicy(
                        min_fit=data.get('min_fit', self._default_policy.min_fit),
                        top_k=data.get('top_k', self._default_policy.top_k),
                        min_jd_required_coverage=data.get(
                            'min_jd_required_coverage',
                            self._default_policy.min_jd_required_coverage
                        )
                    )
        except Exception as e:
            logger.warning(f"Could not load policy from database: {e}")
        
        return self._default_policy
    
    def _save_to_db(self, policy: ResultPolicy) -> None:
        """
        Save policy to database.
        
        Raises:
            Exception: If DB save fails.
        """
        from database.models import AppSettings
        
        with db_session_scope() as session:
            setting = session.query(AppSettings).filter(
                AppSettings.key == 'result_policy'
            ).first()
            
            value = json.dumps({
                'min_fit': policy.min_fit,
                'top_k': policy.top_k,
                'min_jd_required_coverage': policy.min_jd_required_coverage
            })
            
            if setting:
                setting.value = value
            else:
                setting = AppSettings(key='result_policy', value=value)
                session.add(setting)
            
            session.commit()
    
    def _parse_value(self, value):
        """Parse policy value from database."""
        if isinstance(value, str):
            return json.loads(value)
        return value


# Global policy service instance
_policy_service = PolicyService()


def get_policy_service() -> PolicyService:
    """Get the global policy service instance."""
    return _policy_service
