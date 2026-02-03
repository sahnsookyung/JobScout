"""
Tests for Job Cache Service

Tests Redis-based job caching with 2-week TTL.
"""
import pytest
import json
import time
from unittest.mock import Mock, patch, MagicMock

from core.cache.job_cache import JobCacheService, get_job_cache, init_job_cache, CACHE_TTL_SECONDS


class TestJobCacheService:
    """Test suite for JobCacheService."""
    
    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        mock = Mock()
        mock.ping.return_value = True
        mock.get.return_value = None
        mock.setex.return_value = True
        mock.delete.return_value = 1
        mock.dbsize.return_value = 100
        mock.info.return_value = {"used_memory_human": "10M"}
        mock.scan.return_value = (0, [])
        return mock
    
    @pytest.fixture
    def cache_service(self, mock_redis):
        """Create a cache service with mocked Redis."""
        with patch('core.cache.job_cache.Redis') as mock_redis_class:
            mock_redis_class.from_url.return_value = mock_redis
            service = JobCacheService(
                redis_url="redis://localhost:6380/0",
                password="testpass",
                ttl_seconds=1209600
            )
            return service
    
    def test_01_initialization_success(self, mock_redis):
        """Test successful cache initialization with Redis."""
        with patch('core.cache.job_cache.Redis') as mock_redis_class:
            mock_redis_class.from_url.return_value = mock_redis
            
            service = JobCacheService("redis://localhost:6380/0")
            
            assert service.is_available is True
            assert service.ttl_seconds == CACHE_TTL_SECONDS
            mock_redis_class.from_url.assert_called_once()
    
    def test_02_initialization_failure(self):
        """Test graceful handling when Redis is unavailable."""
        with patch('core.cache.job_cache.Redis') as mock_redis_class:
            mock_redis_class.from_url.side_effect = Exception("Connection refused")
            
            service = JobCacheService("redis://localhost:6380/0")
            
            assert service.is_available is False
            assert service._redis is None
    
    def test_03_get_job_cache_hit(self, cache_service, mock_redis):
        """Test retrieving a cached job (cache hit)."""
        job_data = {
            "data": {
                "title": "Software Engineer",
                "company": "Google",
                "description": "Build scalable systems"
            },
            "cached_at": "2024-01-15T10:00:00",
            "ttl_seconds": 1209600
        }
        mock_redis.get.return_value = json.dumps(job_data)
        
        result = cache_service.get_job("abc123")
        
        assert result is not None
        assert result["title"] == "Software Engineer"
        mock_redis.get.assert_called_once_with("job:abc123")
    
    def test_04_get_job_cache_miss(self, cache_service, mock_redis):
        """Test retrieving a non-cached job (cache miss)."""
        mock_redis.get.return_value = None
        
        result = cache_service.get_job("nonexistent")
        
        assert result is None
    
    def test_05_set_job_success(self, cache_service, mock_redis):
        """Test caching a job successfully."""
        job_data = {
            "title": "Software Engineer",
            "company": "Google",
            "description": "Build scalable systems"
        }
        
        result = cache_service.set_job("abc123", job_data)
        
        assert result is True
        mock_redis.setex.assert_called_once()
        # Verify TTL is 2 weeks
        call_args = mock_redis.setex.call_args
        assert call_args[0][1] == CACHE_TTL_SECONDS  # TTL
    
    def test_06_set_job_custom_ttl(self, cache_service, mock_redis):
        """Test caching a job with custom TTL."""
        job_data = {"title": "Test Job"}
        custom_ttl = 3600  # 1 hour
        
        result = cache_service.set_job("abc123", job_data, ttl_seconds=custom_ttl)
        
        assert result is True
        call_args = mock_redis.setex.call_args
        assert call_args[0][1] == custom_ttl
    
    def test_07_delete_job(self, cache_service, mock_redis):
        """Test deleting a job from cache."""
        result = cache_service.delete_job("abc123")
        
        assert result is True
        mock_redis.delete.assert_called_once_with("job:abc123")
    
    def test_08_cache_stats(self, cache_service, mock_redis):
        """Test retrieving cache statistics."""
        # Mock scan to return 100 keys
        # Format: (cursor, keys_list)
        # First call returns 100 keys and cursor 0 (finished)
        mock_keys = [f"job:{i}" for i in range(100)]
        mock_redis.scan.return_value = (0, mock_keys)
        
        stats = cache_service.get_cache_stats()
        
        assert stats["available"] is True
        assert stats["job_cache_keys"] == 100
        assert stats["used_memory_human"] == "10M"
        assert stats["ttl_seconds"] == CACHE_TTL_SECONDS
        assert "14 days" in stats["ttl_human"]
    
    def test_09_clear_all(self, cache_service, mock_redis):
        """Test clearing all cached jobs."""
        mock_redis.scan.return_value = (0, ["job:1", "job:2", "job:3"])
        
        result = cache_service.clear_all()
        
        assert result is True
        mock_redis.delete.assert_called_once_with("job:1", "job:2", "job:3")
    
    def test_10_key_format(self, cache_service, mock_redis):
        """Test that cache keys are formatted correctly."""
        mock_redis.get.return_value = None
        
        cache_service.get_job("fingerprint123")
        
        mock_redis.get.assert_called_once_with("job:fingerprint123")
    
    def test_11_redis_error_handling(self, cache_service, mock_redis):
        """Test graceful handling of Redis errors."""
        mock_redis.get.side_effect = Exception("Redis error")
        
        result = cache_service.get_job("abc123")
        
        assert result is None  # Should return None on error, not crash
    
    def test_12_is_available_ping_failure(self, cache_service, mock_redis):
        """Test is_available returns False when ping fails."""
        mock_redis.ping.side_effect = Exception("Ping failed")
        
        assert cache_service.is_available is False


class TestJobCacheIntegration:
    """Integration tests for job cache with real Redis (if available)."""
    
    @pytest.fixture(scope="class")
    def real_cache(self):
        """Create a cache service connected to real Redis if available."""
        service = JobCacheService("redis://localhost:6380/0")
        if not service.is_available:
            pytest.skip("Redis cache not available")
        # Clear cache before tests
        service.clear_all()
        return service
    
    def test_13_real_cache_round_trip(self, real_cache):
        """Test full cache round-trip with real Redis."""
        fingerprint = "test_job_123"
        job_data = {
            "title": "Software Engineer",
            "company": "Test Corp",
            "description": "Test description"
        }
        
        # Set
        assert real_cache.set_job(fingerprint, job_data) is True
        
        # Get
        cached = real_cache.get_job(fingerprint)
        assert cached is not None
        assert cached["data"]["title"] == "Software Engineer"
        
        # Delete
        assert real_cache.delete_job(fingerprint) is True
        
        # Verify deleted
        assert real_cache.get_job(fingerprint) is None
    
    def test_14_real_cache_ttl_expiration(self, real_cache):
        """Test that cache entries expire after TTL (using short TTL for test)."""
        fingerprint = "test_ttl_job"
        job_data = {"title": "TTL Test Job"}
        short_ttl = 1  # 1 second
        
        # Cache with 1 second TTL
        real_cache.set_job(fingerprint, job_data, ttl_seconds=short_ttl)
        
        # Should exist immediately
        assert real_cache.get_job(fingerprint) is not None
        
        # Wait for expiration
        time.sleep(2)
        
        # Should be expired
        assert real_cache.get_job(fingerprint) is None
    
    def test_15_real_cache_stats(self, real_cache):
        """Test cache stats with real Redis."""
        stats = real_cache.get_cache_stats()
        
        assert stats["available"] is True
        assert "used_memory_human" in stats
        assert "ttl_human" in stats


class TestGlobalJobCache:
    """Tests for global job cache instance."""
    
    def test_16_init_job_cache(self):
        """Test initializing global job cache."""
        with patch('core.cache.job_cache.JobCacheService') as mock_service_class:
            mock_instance = Mock()
            mock_instance.is_available = True
            mock_service_class.return_value = mock_instance
            
            result = init_job_cache("redis://localhost:6380/0", "password")
            
            assert result == mock_instance
            mock_service_class.assert_called_once_with("redis://localhost:6380/0", "password")
    
    def test_17_get_job_cache_after_init(self):
        """Test getting job cache after initialization."""
        with patch('core.cache.job_cache.JobCacheService') as mock_service_class:
            mock_instance = Mock()
            mock_instance.is_available = True
            mock_service_class.return_value = mock_instance
            
            init_job_cache("redis://localhost:6380/0")
            cache = get_job_cache()
            
            assert cache == mock_instance
    
    def test_18_get_job_cache_before_init(self):
        """Test getting job cache before initialization returns None."""
        # Reset global state
        import core.cache.job_cache as jc
        jc._job_cache = None
        
        cache = get_job_cache()
        assert cache is None
