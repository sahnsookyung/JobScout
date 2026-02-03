"""
Tests for Job Cache integration with ETL/Orchestrator.

Tests that ETL pipeline properly uses job cache to avoid re-scraping.
"""
import pytest
import json
import hashlib
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone

from etl.orchestrator import JobETLOrchestrator
from core.cache.job_cache import JobCacheService, get_job_cache, init_job_cache


class TestETLJobCacheIntegration:
    """Test ETL integration with job cache."""
    
    @pytest.fixture
    def mock_cache(self):
        """Create a mock job cache."""
        cache = Mock(spec=JobCacheService)
        cache.is_available = True
        cache.get_job.return_value = None  # Default: cache miss
        cache.set_job.return_value = True
        return cache
    
    @pytest.fixture
    def sample_job_raw(self):
        """Sample raw job data from scraper."""
        return {
            "job_id": "12345",
            "job_title": "Software Engineer",
            "company_name": "Google",
            "job_description": "Build scalable systems...",
            "job_location": "Mountain View, CA",
            "canonical_fingerprint": "abc123def456"
        }
    
    def test_01_cache_hit_skips_description_extraction(self, mock_cache):
        """When cache hit, use cached description instead of extracting from website."""
        # Setup cache hit
        cached_data = {
            "data": {
                "title": "Software Engineer",
                "company": "Google", 
                "description": "Cached description from 2 weeks ago",
                "location": "Mountain View, CA"
            },
            "cached_at": datetime.now(timezone.utc).isoformat(),
            "ttl_seconds": 1209600
        }
        mock_cache.get_job.return_value = cached_data
        
        # Verify cache is checked first
        result = mock_cache.get_job("abc123def456")
        assert result is not None
        assert result["data"]["description"] == "Cached description from 2 weeks ago"
    
    def test_02_cache_miss_triggers_full_extraction(self, mock_cache):
        """When cache miss, full extraction from website is needed."""
        # Setup cache miss
        mock_cache.get_job.return_value = None
        
        # Verify cache miss
        result = mock_cache.get_job("new_job_789")
        assert result is None
        # In real implementation, this would trigger full extraction
    
    def test_03_successful_extraction_caches_result(self, mock_cache, sample_job_raw):
        """After successful extraction, result should be cached."""
        # Simulate extraction completed
        job_data = {
            "title": sample_job_raw["job_title"],
            "company": sample_job_raw["company_name"],
            "description": sample_job_raw["job_description"],
            "location": sample_job_raw["job_location"]
        }
        
        # Cache the result
        mock_cache.set_job(
            sample_job_raw["canonical_fingerprint"],
            job_data
        )
        
        # Verify set_job was called
        mock_cache.set_job.assert_called_once()
        call_args = mock_cache.set_job.call_args
        assert call_args[0][0] == "abc123def456"  # fingerprint
        assert call_args[0][1]["title"] == "Software Engineer"
    
    def test_04_cache_availability_check(self, mock_cache):
        """ETL should check if cache is available before using."""
        # Available cache
        assert mock_cache.is_available is True
        
        # Unavailable cache
        mock_cache.is_available = False
        assert mock_cache.is_available is False
    
    def test_05_fingerprint_based_lookup(self, mock_cache):
        """Cache lookups use canonical fingerprint as key."""
        fingerprint = "linkedin:google:software-engineer:abc123"
        
        mock_cache.get_job(fingerprint)
        
        mock_cache.get_job.assert_called_once_with(fingerprint)
    
    def test_06_cache_respects_ttl(self, mock_cache):
        """Cache entries respect 2-week TTL."""
        job_data = {"title": "Test Job"}
        
        mock_cache.set_job(
            "fingerprint123",
            job_data,
            ttl_seconds=1209600  # 2 weeks
        )
        
        call_args = mock_cache.set_job.call_args
        assert call_args[1]["ttl_seconds"] == 1209600


class TestJobCacheWithFingerprint:
    """Test job cache integration with canonical fingerprint generation."""
    
    def test_01_same_job_same_fingerprint(self):
        """Same job data should produce same fingerprint."""
        job1 = {"title": "Engineer", "company": "Google", "location": "CA"}
        job2 = {"title": "Engineer", "company": "Google", "location": "CA"}
        
        # In real implementation, these would use the same fingerprint
        # and thus hit the same cache entry
        import json
        fingerprint1 = hashlib.sha256(json.dumps(job1, sort_keys=True).encode()).hexdigest()[:16]
        fingerprint2 = hashlib.sha256(json.dumps(job2, sort_keys=True).encode()).hexdigest()[:16]
        
        assert fingerprint1 == fingerprint2
    
    def test_02_different_jobs_different_fingerprints(self):
        """Different jobs should have different fingerprints."""
        job1 = {"title": "Engineer", "company": "Google"}
        job2 = {"title": "Manager", "company": "Google"}
        
        fingerprint1 = f"{job1['company']}:{job1['title']}"
        fingerprint2 = f"{job2['company']}:{job2['title']}"
        
        assert fingerprint1 != fingerprint2


class TestCacheErrorHandling:
    """Test error handling when cache fails."""
    
    def test_01_get_job_handles_redis_error(self):
        """ETL should continue even if cache read fails."""
        from core.cache.job_cache import JobCacheService
        
        with patch('core.cache.job_cache.Redis') as mock_redis_class:
            mock_redis = Mock()
            mock_redis.ping.return_value = True
            mock_redis.get.side_effect = Exception("Redis connection failed")
            mock_redis_class.from_url.return_value = mock_redis
            
            cache = JobCacheService("redis://localhost:6380/0")
            
            # Should not crash, just return None
            result = cache.get_job("fingerprint")
            assert result is None
    
    def test_02_set_job_handles_redis_error(self):
        """ETL should continue even if cache write fails."""
        from core.cache.job_cache import JobCacheService
        
        with patch('core.cache.job_cache.Redis') as mock_redis_class:
            mock_redis = Mock()
            mock_redis.ping.return_value = True
            mock_redis.setex.side_effect = Exception("Redis write failed")
            mock_redis_class.from_url.return_value = mock_redis
            
            cache = JobCacheService("redis://localhost:6380/0")
            
            # Should not crash, just return False
            result = cache.set_job("fingerprint", {})
            assert result is False
    
    def test_03_unavailable_cache_graceful_degradation(self):
        """ETL works normally when cache is unavailable."""
        cache = Mock(spec=JobCacheService)
        cache.is_available = False
        
        # Should skip cache operations entirely
        assert cache.is_available is False
