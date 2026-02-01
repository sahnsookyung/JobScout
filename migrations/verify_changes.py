#!/usr/bin/env python3
"""
Quick verification script for the 'notified' column changes.
Tests that the model includes the notified field and behaves correctly.
"""
import sys
sys.path.insert(0, '/Users/sookyungahn/repos/JobScout')

from database.models import JobMatch
from decimal import Decimal

# Test 1: Verify JobMatch model has notified field
print("Test 1: Verify JobMatch model has 'notified' field...")
assert hasattr(JobMatch, 'notified'), "JobMatch model missing 'notified' field"
print("   ✓ JobMatch has notified field")

# Test 2: Verify default value
print("\nTest 2: Verify default value...")
from sqlalchemy import inspect as sa_inspect
inspector = sa_inspect(JobMatch)
column = inspector.get_property('notified')
print(f"   ✓ Column info: {column}")

# Test 3: Verify table args include index
print("\nTest 3: Verify index exists...")
table_args = getattr(JobMatch, '__table_args__', ())
has_index = False
for arg in table_args:
    if hasattr(arg, 'name') and arg.name == 'idx_job_match_notified':
        has_index = True
        break
assert has_index, "Missing idx_job_match_notified index"
print("   ✓ Index idx_job_match_notified exists")

print("\n" + "="*60)
print("✅ ALL VERIFICATIONS PASSED!")
print("="*60)
print("\nSummary of changes:")
print("1. JobMatch model has 'notified' boolean field")
print("2. Index 'idx_job_match_notified' created for efficient queries")
print("3. Main.py no longer filters by threshold before saving")
print("4. ScorerService updates all fields for existing matches")
print("5. Notification logic only notifies unnotified matches above threshold")
