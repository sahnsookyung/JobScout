"""Job Cache Service - Redis caching for job data."""
import json
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timezone
from urllib.parse import urlparse

try:
    from redis import Redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    Redis = None

logger = logging.getLogger(__name__)

# 2 weeks in seconds
CACHE_TTL_SECONDS = 14 * 24 * 60 * 60  # 1209600 seconds


def _sanitize_url(url: str) -> str:
    """Remove credentials from URL for safe logging."""
    try:
        parsed = urlparse(url)
        if parsed.password:
            sanitized = parsed._replace(
                netloc=f"{parsed.username or ''}:*@{parsed.hostname}:{parsed.port or 6379}"
            )
            return sanitized.geturl()
        return url
    except Exception:
        return url


class JobCacheService:
    """
    Service for caching job data to avoid re-scraping.
    
    Uses Redis with 2-week TTL. Jobs are cached by canonical fingerprint.
    """
    
    def __init__(
        self,
        redis_url: str = "redis://localhost:6380/0",
        password: Optional[str] = None,
        ttl_seconds: int = CACHE_TTL_SECONDS
    ):
        self.redis_url = redis_url
        self.password = password
        self.ttl_seconds = ttl_seconds
        self._redis: Optional[Redis] = None
        self._available = False
        
        if REDIS_AVAILABLE:
            try:
                self._redis = Redis.from_url(
                    redis_url,
                    password=password,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5
                )
                self._redis.ping()
                self._available = True
                logger.info(f"Job cache connected to Redis at {_sanitize_url(redis_url)}")
            except Exception as e:
                logger.warning(f"Job cache Redis unavailable: {e}")
                self._redis = None
                self._available = False
        else:
            logger.warning("Redis not installed, job cache disabled")
    
    @property
    def is_available(self) -> bool:
        """Check if cache is available."""
        if not self._available or not self._redis:
            return False
        try:
            return self._redis.ping()
        except Exception:
            return False
    
    def _make_key(self, canonical_fingerprint: str) -> str:
        """Create cache key from fingerprint."""
        return f"job:{canonical_fingerprint}"
    
    def get_job(self, canonical_fingerprint: str) -> Optional[Dict[str, Any]]:
        """Get cached job data by canonical fingerprint."""
        if not self.is_available:
            return None

        try:
            key = self._make_key(canonical_fingerprint)
            data = self._redis.get(key)

            if data:
                cache_entry = json.loads(data)
                logger.debug(f"Cache hit for job {canonical_fingerprint[:16]}...")
                return cache_entry.get("data")
            else:
                logger.debug(f"Cache miss for job {canonical_fingerprint[:16]}...")
                return None

        except Exception as e:
            logger.warning(f"Error reading from job cache: {e}")
            return None
    
    def set_job(
        self,
        canonical_fingerprint: str,
        job_data: Dict[str, Any],
        ttl_seconds: Optional[int] = None
    ) -> bool:
        """Cache job data with TTL."""
        if not self.is_available:
            return False
        
        try:
            key = self._make_key(canonical_fingerprint)
            ttl = ttl_seconds or self.ttl_seconds
            
            cache_entry = {
                "data": job_data,
                "cached_at": datetime.now(timezone.utc).isoformat(),
                "ttl_seconds": ttl
            }
            
            self._redis.setex(key, ttl, json.dumps(cache_entry))
            logger.debug(f"Cached job {canonical_fingerprint[:16]}... (TTL: {ttl}s)")
            return True
            
        except Exception as e:
            logger.warning(f"Error writing to job cache: {e}")
            return False
    
    def delete_job(self, canonical_fingerprint: str) -> bool:
        """Remove job from cache."""
        if not self.is_available:
            return False
        
        try:
            key = self._make_key(canonical_fingerprint)
            self._redis.delete(key)
            logger.debug(f"Deleted job {canonical_fingerprint[:16]}... from cache")
            return True
        except Exception as e:
            logger.warning(f"Error deleting from job cache: {e}")
            return False
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        if not self.is_available:
            return {"available": False}

        try:
            info = self._redis.info()
            job_key_count = 0
            cursor = 0
            while True:
                cursor, keys = self._redis.scan(cursor=cursor, match="job:*", count=1000)
                job_key_count += len(keys)
                if cursor == 0:
                    break
            return {
                "available": True,
                "used_memory_human": info.get("used_memory_human", "unknown"),
                "job_cache_keys": job_key_count,
                "ttl_seconds": self.ttl_seconds,
                "ttl_human": f"{self.ttl_seconds // 86400} days"
            }
        except Exception as e:
            logger.warning(f"Error getting cache stats: {e}")
            return {"available": False, "error": str(e)}
    
    def clear_all(self) -> bool:
        """Clear all cached jobs. Use with caution."""
        if not self.is_available:
            return False
        
        try:
            pattern = "job:*"
            cursor = 0
            deleted = 0
            
            while True:
                cursor, keys = self._redis.scan(cursor=cursor, match=pattern, count=100)
                if keys:
                    self._redis.delete(*keys)
                    deleted += len(keys)
                if cursor == 0:
                    break
            
            logger.info(f"Cleared {deleted} jobs from cache")
            return True
            
        except Exception as e:
            logger.warning(f"Error clearing job cache: {e}")
            return False


# Global instance for application use
_job_cache: Optional[JobCacheService] = None


def get_job_cache() -> Optional[JobCacheService]:
    """Get global job cache instance."""
    return _job_cache


def init_job_cache(redis_url: str, password: Optional[str] = None) -> JobCacheService:
    """Initialize global job cache."""
    global _job_cache
    _job_cache = JobCacheService(redis_url, password)
    return _job_cache
