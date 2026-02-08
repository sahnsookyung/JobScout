
import os
import time
import threading
from pipeline.control import PipelineController

def test_locking():
    print("Testing PipelineController locking...")
    
    # 1. Acquire lock in main thread
    c1 = PipelineController("test.lock")
    if c1.acquire_lock("process1"):
        print("PASS: Acquired lock 1")
    else:
        print("FAIL: Could not acquire lock 1")
        return

    # 2. Try to acquire in another thread/controller (simulating another process)
    def try_acquire():
        c2 = PipelineController("test.lock")
        if c2.acquire_lock("process2"):
            print("FAIL: Acquired lock 2 while lock 1 is held")
        else:
            print("PASS: Correctly denied lock 2")

    t = threading.Thread(target=try_acquire)
    t.start()
    t.join()

    # 3. Release lock 1
    c1.release_lock()
    print("Released lock 1")

    # 4. Acquire lock 2
    c2 = PipelineController("test.lock")
    if c2.acquire_lock("process2"):
        print("PASS: Acquired lock 2 after release")
    else:
        print("FAIL: Could not acquire lock 2 after release")

    c2.release_lock()
    
    # Clean up
    if os.path.exists("test.lock"):
        os.remove("test.lock")

if __name__ == "__main__":
    test_locking()
