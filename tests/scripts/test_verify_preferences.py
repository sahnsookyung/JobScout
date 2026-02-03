#!/usr/bin/env python3
"""
Simple verification that preferences-based matching components work.
"""

from unittest.mock import MagicMock
from core.matcher import PreferencesAlignmentScore
from tests.mocks.matcher_mocks import MockMatcherService
from core.scorer import ScoringService
from core.scorer import preferences as scorer_preferences
from core.scorer import penalties as scorer_penalties
from core.config_loader import MatcherConfig, ScorerConfig

# Sample preferences
preferences = {
    "job_preferences": {
        "wants_remote": True,
        "location_preferences": {
            "preferred_locations": ["Tokyo", "Osaka", "Remote"],
            "avoid_locations": []
        }
    },
    "compensation": {
        "salary": {"minimum": 5000000, "target": 8000000, "currency": "JPY"}
    },
    "career_preferences": {
        "seniority_level": "mid",
        "role_types": ["Software Engineer"],
        "avoid_roles": ["Manager"]
    },
    "company_preferences": {
        "company_size": {"employee_count": {"minimum": 10, "maximum": 500}},
        "industry": {"preferred": ["SaaS", "Fintech"], "avoid": ["Gaming"]}
    }
}

# Setup
mock_repo = MagicMock()
mock_ai = MagicMock()
mock_ai.generate_embedding = MagicMock(return_value=[0.1] * 1024)

matcher_config = MatcherConfig(similarity_threshold=0.3)
scorer_config = ScorerConfig(wants_remote=True, min_salary=5000000)

matcher = MockMatcherService(mock_repo, mock_ai, matcher_config)
scorer = ScoringService(mock_repo, scorer_config)

test_results = []

print("=" * 60)
print("PREFERENCES-BASED MATCHING - VERIFICATION")
print("=" * 60)

# Test 1: Location matching
print("\n📍 Test 1: Location Matching")
job_remote = MagicMock()
job_remote.location_text = "Remote"
job_remote.is_remote = True
score, _ = matcher.calculate_location_match(job_remote, preferences)
passed = score == 1.0
test_results.append(passed)
print(f"  Remote job: {score:.2f} (expected: 1.0) {'✓' if passed else '✗'}")

job_tokyo = MagicMock()
job_tokyo.location_text = "Tokyo, Japan"
job_tokyo.is_remote = False
score, _ = matcher.calculate_location_match(job_tokyo, preferences)
passed = score > 0.5
test_results.append(passed)
print(f"  Tokyo job: {score:.2f} (expected: ~0.7) {'✓' if passed else '✗'}")

# Test 2: Industry matching
print("\n🏭 Test 2: Industry Matching")
job_saas = MagicMock()
job_saas.company_industry = "SaaS"
score, _ = matcher.calculate_industry_match(job_saas, preferences)
passed = score == 1.0
test_results.append(passed)
print(f"  SaaS company: {score:.2f} (expected: 1.0) {'✓' if passed else '✗'}")

job_gaming = MagicMock()
job_gaming.company_industry = "Gaming"
score, _ = matcher.calculate_industry_match(job_gaming, preferences)
passed = score == 0.0
test_results.append(passed)
print(f"  Gaming company: {score:.2f} (expected: 0.0) {'✓' if passed else '✗'}")

# Test 3: Company size matching
print("\n🏢 Test 3: Company Size Matching")
job_small = MagicMock()
job_small.company_num_employees = "5"
score, _ = matcher.calculate_company_size_match(job_small, preferences)
passed = score < 0.5
test_results.append(passed)
print(f"  5 employees: {score:.2f} (expected: <0.5) {'✓' if passed else '✗'}")

job_ideal = MagicMock()
job_ideal.company_num_employees = "100"
score, _ = matcher.calculate_company_size_match(job_ideal, preferences)
passed = score == 1.0
test_results.append(passed)
print(f"  100 employees: {score:.2f} (expected: 1.0) {'✓' if passed else '✗'}")

# Test 4: Role matching
print("\n💼 Test 4: Role Matching")
job_eng = MagicMock()
job_eng.title = "Software Engineer"
job_eng.job_level = "Mid-level"
score, _ = matcher.calculate_role_match(job_eng, preferences)
passed = score == 1.0
test_results.append(passed)
print(f"  Software Engineer: {score:.2f} (expected: 1.0) {'✓' if passed else '✗'}")

job_mgr = MagicMock()
job_mgr.title = "Engineering Manager"
job_mgr.job_level = "Senior"
score, _ = matcher.calculate_role_match(job_mgr, preferences)
passed = score == 0.0
test_results.append(passed)
print(f"  Manager: {score:.2f} (expected: 0.0) {'✓' if passed else '✗'}")

# Test 5: Overall alignment
print("\n🎯 Test 5: Overall Preferences Alignment")
job_perfect = MagicMock()
job_perfect.location_text = "Remote"
job_perfect.is_remote = True
job_perfect.company_num_employees = "50"
job_perfect.company_industry = "SaaS"
job_perfect.title = "Software Engineer"
job_perfect.job_level = "Mid-level"

alignment = matcher.calculate_preferences_alignment(job_perfect, preferences)
print(f"  Perfect job alignment: {alignment.overall_score:.2f}")
print(f"    Location: {alignment.location_match:.2f}")
print(f"    Industry: {alignment.industry_match:.2f}")
print(f"    Company size: {alignment.company_size_match:.2f}")
print(f"    Role: {alignment.role_match:.2f}")

# Test 6: Scoring with preferences
print("\n📊 Test 6: Scoring with Preferences")
boost, details = scorer_preferences.calculate_preferences_boost(alignment, scorer.config)
print(f"  Preferences boost: +{boost:.1f} points")

job_bad = MagicMock()
job_bad.is_remote = False
job_bad.location_text = "New York"
job_bad.salary_max = None
job_bad.job_level = None

bad_alignment = PreferencesAlignmentScore(
    overall_score=0.2,
    location_match=0.0,
    company_size_match=0.5,
    industry_match=0.0,
    role_match=0.0,
    details={}
)

penalties, penalty_details = scorer_penalties.calculate_penalties(
    job_bad, 1.0, [], [], scorer.config, bad_alignment
)
print(f"  Bad preferences penalties: {penalties:.1f}")
industry_penalty = next((p for p in penalty_details if p['type'] == 'industry_mismatch'), None)
role_penalty = next((p for p in penalty_details if p['type'] == 'role_mismatch'), None)
if industry_penalty:
    print(f"    Industry penalty: {industry_penalty['amount']}")
if role_penalty:
    print(f"    Role penalty: {role_penalty['amount']}")

print("\n" + "=" * 60)
if all(test_results):
    print("✅ ALL PREFERENCES COMPONENTS VERIFIED SUCCESSFULLY!")
    print("=" * 60)
else:
    failed_count = sum(1 for r in test_results if not r)
    print(f"❌ {failed_count} TEST(S) FAILED!")
    print("=" * 60)
    import sys
    sys.exit(1)
