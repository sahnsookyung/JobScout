
import requests
import time
import sys

BASE_URL = "http://localhost:8080"

def test_pipeline_status():
    print(f"Triggering pipeline at {BASE_URL}/api/pipeline/run-matching...")
    try:
        response = requests.post(f"{BASE_URL}/api/pipeline/run-matching")
        if response.status_code != 200:
            print(f"Failed to start pipeline: {response.text}")
            return
        
        data = response.json()
        task_id = data["task_id"]
        print(f"Pipeline started with task_id: {task_id}")
        
        last_step = None
        MAX_POLL_TIME = 300  # 5 minutes timeout
        start_time = time.time()
        while True:
            if time.time() - start_time > MAX_POLL_TIME:
                print(f"Timeout: Pipeline did not complete within {MAX_POLL_TIME} seconds")
                break
            
            status_resp = requests.get(f"{BASE_URL}/api/pipeline/status/{task_id}")
            if status_resp.status_code != 200:
                print(f"Failed to get status: {status_resp.text}")
                break
            
            status_data = status_resp.json()
            status = status_data["status"]
            step = status_data.get("step")
            
            if step != last_step:
                print(f"Status: {status}, Step: {step}")
                last_step = step
            
            if status in ["completed", "failed"]:
                print(f"Final Status: {status}")
                if status == "failed":
                    print(f"Error: {status_data.get('error')}")
                else:
                    print(f"Matches: {status_data.get('matches_count')}")
                    print(f"Saved: {status_data.get('saved_count')}")
                break
            
            time.sleep(0.5)
            
    except requests.exceptions.ConnectionError:
        print("Could not connect to server. Is it running?")

def test_singleton_concurrency():
    try:
        print("\n=== Testing Singleton Pattern ===")
        
        # Check active status (should be None or running if previous run hasn't finished)
        active_resp = requests.get(f"{BASE_URL}/api/pipeline/active")
        print(f"Initial Active Task Check: {active_resp.json()}")

        # Trigger first run
        print("Triggering Run 1...")
        resp1 = requests.post(f"{BASE_URL}/api/pipeline/run-matching")
        task1 = resp1.json()["task_id"]
        print(f"Run 1 Task ID: {task1}")

        # Trigger second run immediately
        print("Triggering Run 2 (should return Run 1 ID)...")
        resp2 = requests.post(f"{BASE_URL}/api/pipeline/run-matching")
        task2 = resp2.json()["task_id"]
        print(f"Run 2 Task ID: {task2}")
        
        if task1 == task2:
            print("SUCCESS: Task IDs match. Singleton pattern working.")
        else:
            print("FAILURE: Task IDs do not match!")

        # Check active status again
        active_resp = requests.get(f"{BASE_URL}/api/pipeline/active")
        active_task = active_resp.json()
        print(f"Active Task Check: {active_task}")
        
        if active_task and active_task['task_id'] == task1:
            print("SUCCESS: Active task endpoint returned correct task.")
        else:
            print("FAILURE: Active task endpoint failed.")
    except requests.exceptions.ConnectionError:
        print("Could not connect to server. Is it running?")

def test_cancellation():
    try:
        print("\n=== Testing Pipeline Cancellation ===")
        
        # Trigger run
        print("Triggering Run for Cancellation...")
        resp = requests.post(f"{BASE_URL}/api/pipeline/run-matching")
        task_id = resp.json()["task_id"]
        print(f"Task ID: {task_id}")
        
        # Wait a bit to let it start
        time.sleep(2)
        
        # Send stop request
        print("Sending Stop Request...")
        stop_resp = requests.post(f"{BASE_URL}/api/pipeline/stop")
        print(f"Stop Response: {stop_resp.json()}")
        
        # Poll status until finished
        MAX_POLL_TIME = 60  # 1 minute timeout for cancellation
        start_time = time.time()
        while True:
            if time.time() - start_time > MAX_POLL_TIME:
                print(f"Timeout: Pipeline did not terminate within {MAX_POLL_TIME} seconds")
                break

            status_resp = requests.get(f"{BASE_URL}/api/pipeline/status/{task_id}")
            status_data = status_resp.json()
            status = status_data["status"]
            
            print(f"Status: {status}")
            
            if status in ["completed", "failed"]:
                print(f"Final Status: {status}")
                if status == "failed":
                    print(f"Error: {status_data.get('error')}")
                    if "Interrupted by system" in str(status_data.get('error')):
                        print("SUCCESS: Pipeline interrupted by system error found.")
                    else:
                        print("WARNING: Pipeline failed but not with 'Interrupted by system' error.")
                else:
                    print("FAILURE: Pipeline completed instead of stopping.")
                break
            
            time.sleep(1)
    except requests.exceptions.ConnectionError:
        print("Could not connect to server. Is it running?")

if __name__ == "__main__":
    # test_pipeline_status()
    # test_singleton_concurrency()
    test_cancellation()
