#!/usr/bin/env python3
"""
Test script for API server functionality.
Tests multiple scrapers to verify the generic implementation.
"""
import requests
import json
import time

API_URL = "http://localhost:8000"

def submit_job(site_type, search_term=None, location=None, results_wanted=3, options=None, **kwargs):
    """Submit a scraping job and return the task ID."""
    # Construct payload matching ScraperInput structure
    payload = {
        "site_type": site_type,
        "search_term": search_term,
        "location": location,
        "results_wanted": results_wanted,
        "is_remote": False,
        "options": options or {},
        **kwargs # Pass other top-level fields like hours_old
    }
    
    print(f"\n{'='*60}")
    print(f"Testing {site_type}")
    print(f"{'='*60}")
    print(f"Request: {json.dumps(payload, indent=2)}")
    
    try:
        response = requests.post(f"{API_URL}/scrape", json=payload, timeout=10)
        print(f"Response Code: {response.status_code}")
        response.raise_for_status()
        result = response.json()
        print(f"✓ Job submitted: {result['task_id']}")
        return result['task_id']
    except Exception as e:
        print(f"✗ Failed to submit job: {e}")
        try:
             print(f"Response content: {response.text}")
        except:
             pass
        return None

def check_status(task_id, max_wait=120):
    """Check job status and wait for completion."""
    if not task_id:
        return None
        
    start_time = time.time()
    while time.time() - start_time < max_wait:
        try:
            response = requests.get(f"{API_URL}/status/{task_id}", timeout=10)
            response.raise_for_status()
            result = response.json()
            
            status = result.get('status')
            if status == 'completed':
                count = result.get('count', 0)
                data = result.get('data', [])
                print(f"✓ Completed: Found {count} jobs")
                
                # Show head of results (first 2 jobs)
                if data:
                    print("-" * 20)
                    print("HEAD OF RESULTS (First 3 jobs):")
                    for i, job in enumerate(data[:3]):
                        print(f"[{i+1}] {job.get('title')} @ {job.get('company_name')}")
                        print(f"    Location: {job.get('location')}")
                        print(f"    URL: {job.get('job_url')}")
                    print("-" * 20)
                
                return result
            elif status == 'failed':
                error = result.get('error', 'Unknown error')
                print(f"✗ Failed: {error}")
                return result
            else:
                print(f"  Status: {status}... (waiting)")
                time.sleep(5)
        except Exception as e:
            print(f"✗ Error checking status: {e}")
            return None
    
    print(f"✗ Timeout after {max_wait}s")
    return None

def test_scrapers():
    """Test multiple scrapers."""
    
    tests = [
        {
            "site_type": ["tokyodev"],
            "search_term": "python",
            "results_wanted": 3,
            "options": {
                "japanese_requirements": ["none"],
                "seniorities": ["intern", "junior", "intermediate"]
            }
        },
        {
            "site_type": ["japandev"],
            "search_term": "",
            "results_wanted": 3,
            "options": {
                "japanese_levels": ["japanese_level_not_required"],
                "seniorities": ["seniority_level_junior", "seniority_level_mid_level"],
                "applicant_locations": ["candidate_location_anywhere"]
            }
        },
        {
            "site_type": ["indeed"],
            "search_term": "software engineer",
            "location": "Tokyo",
            "country": "Japan",
            "results_wanted": 3,
            "hours_old": 24*7,
            "options": {}
        },
        {
            "site_type": ["linkedin"],
            "search_term": "software engineer",
            "location": "Tokyo",
            "results_wanted": 3,
            "hours_old": 168,
            "linkedin_fetch_description": True,
            "options": {}
        },
        {
            "site_type": ["glassdoor"],
            "search_term": "software engineer",
            "location": "Tokyo",
            "results_wanted": 3,
            "hours_old": 168,
            "request_timeout": 60*5, # 5 mins
            "options": {}
        }
    ]
    
    results = []
    for test in tests:
        task_id = submit_job(**test)
        if task_id:
            result = check_status(task_id, max_wait=180)
            results.append({
                "site": str(test["site_type"]),
                "task_id": task_id,
                "result": result
            })
        time.sleep(2)
    
    # Summary
    print(f"\n{'='*60}")
    print("TEST SUMMARY")
    print(f"{'='*60}")
    for r in results:
        status = r['result'].get('status', 'unknown') if r['result'] else 'no result'
        count = r['result'].get('count', 0) if r['result'] else 0
        print(f"{r['site']:30} - {status:15} ({count} jobs)")

if __name__ == "__main__":
    print("Starting API tests...")
    print("Make sure the API server is running on http://localhost:8000")
    print("\nWaiting 3 seconds before starting tests...")
    time.sleep(3)
    test_scrapers()
