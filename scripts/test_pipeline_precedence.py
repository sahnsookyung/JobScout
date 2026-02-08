
import requests
import time
import subprocess
import threading
import sys
import os

BASE_URL = "http://localhost:8080"
PIPELINE_LOCK_FILE = "pipeline.lock"

def test_precedence():
    print("\n=== Testing Pipeline Precedence ===")

    # 1. Start Frontend Job
    print("1. Triggering Frontend Pipeline...")
    try:
        resp = requests.post(f"{BASE_URL}/api/pipeline/run-matching")
        if resp.status_code != 200:
            print(f"Failed to start frontend pipeline: {resp.text}")
            return
        task_id = resp.json()["task_id"]
        print(f"Frontend Task ID: {task_id}")
    except Exception as e:
        print(f"Error starting frontend pipeline: {e}")
        return

    # Wait a bit for it to lock
    time.sleep(2)

    # 2. Run main.py in a separate process
    # We use a timeout to ensure it doesn't hang forever if locking is broken
    print("2. Starting main.py (simulating scheduled run)...")
    
    # Run main.py in 'matching' mode for speed
    cmd = ["uv", "run", "python", "main.py", "--mode", "matching"]
    
    start_time = time.time()
    process = subprocess.Popen(
        cmd, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        text=True
    )

    # Monitor output for preemption messages
    preempted = False
    lock_acquired = False
    
    while True:
        # Check if main.py finished
        ret = process.poll()
        if ret is not None:
            break
            
        # Read output non-blocking (simplified here by just reading line by line if available)
        # For simplicity in this script, we'll just wait for it to finish or timeout
        # But we need to check the FRONTEND status concurrently
        
        try:
            status_resp = requests.get(f"{BASE_URL}/api/pipeline/status/{task_id}", timeout=1)
            status = status_resp.json()["status"]
            error = status_resp.json().get("error", "")
            
            if status == "failed" and "Stopped by user" in error:
                print("SUCCESS: Frontend pipeline was cancelled (Stopped by user).")
                preempted = True
                break
            
            if status == "completed":
                print("WARNING: Frontend pipeline completed before main.py could preempt it.")
                break
                
        except Exception:
            pass
            
        if time.time() - start_time > 120:
            print("TIMEOUT: Waiting for preemption.")
            process.terminate()
            stdout, stderr = process.communicate()
            print("--- Main Output ---")
            print(stdout)
            print("--- Main Error ---")
            print(stderr)
            return

        time.sleep(1)

    # Wait for main.py to finish its cycle (it should acquire lock and run)
    print("Waiting for main.py to finish...")
    stdout, stderr = process.communicate(timeout=60)
    
    if "Lock acquired after preemption" in stdout or "Lock acquired after preemption" in stderr:
        print("SUCCESS: main.py reported acquiring lock after preemption.")
    else:
        print("WARNING: Could not find lock acquisition message in main.py output.")
        # print("Main Output:\n", stdout)
        # print("Main Error:\n", stderr)

    if preempted:
        print("\n=== Precedence Test PASSED ===")
    else:
        print("\n=== Precedence Test FAILED ===")

if __name__ == "__main__":
    test_precedence()
