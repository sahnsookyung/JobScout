# Job Matching - Preferences File

This directory contains example preference files for the job matching pipeline.

## Quick Start

1. **Copy the example file:**
   ```bash
   cp preferences.example.json preferences.json
   ```

2. **Edit preferences.json** with your specific preferences

3. **Update config.yaml** to use preferences mode:
   ```yaml
   matching:
     enabled: true
     mode: "with_preferences"  # Change from "requirements_only"
     resume_file: "resume.json"
     preferences_file: "preferences.json"  # Point to your file
   ```

4. **Run the pipeline:**
   ```bash
   python main.py
   ```

## File Structure

### `preferences.json` Structure

```json
{
  "job_preferences": {
    "wants_remote": true,
    "location_preferences": {
      "preferred_locations": ["Tokyo", "Osaka", "Remote"],
      "avoid_locations": []
    }
  },
  
  "compensation": {
    "salary": {
      "minimum": 5000000,
      "target": 8000000,
      "currency": "JPY"
    }
  },
  
  "career_preferences": {
    "seniority_level": "mid",
    "role_types": ["Software Engineer", "Backend Developer"],
    "avoid_roles": ["Manager", "Team Lead"]
  },
  
  "technical_preferences": {
    "primary_languages": ["Python", "Java", "TypeScript"],
    "avoid_technologies": ["PHP", "WordPress"]
  },
  
  "company_preferences": {
    "company_size": {
      "employee_count": {"minimum": 10, "maximum": 500}
    },
    "industry": {
      "preferred": ["SaaS", "Fintech", "AI/ML"],
      "avoid": ["Gaming", "AdTech", "Crypto"]
    }
  },
  
  "priorities": {
    "ranking": [
      "remote_flexibility",
      "technical_growth", 
      "compensation"
    ],
    "dealbreakers": ["no_remote_option"]
  }
}
```

## How It Works

### Matching Modes

**1. `requirements_only` (Default)**
- Matches your resume skills to job requirements
- Uses: `resume.json` only
- Best for: Quick skill-based matching

**2. `with_preferences`**
- Matches resume skills + checks job against your preferences
- Uses: `resume.json` + `preferences.json`
- Best for: Personalized job discovery

### Preferences Alignment Calculation

The matching pipeline calculates an **alignment score** (0.0-1.0) for each job based on your preferences:

```
Overall Alignment = 0.35*Location + 0.25*Industry + 0.25*Role + 0.15*CompanySize
```

#### Location Matching (Weight: 35%)
- **Perfect (1.0)**: Remote job when you want remote
- **Good (0.7)**: Preferred location but not remote
- **Okay (0.3-0.6)**: Acceptable location
- **Bad (0.0)**: Avoided location or doesn't meet remote requirement

#### Industry Matching (Weight: 25%)
- **Perfect (1.0)**: Job in preferred industry
- **Avoided (0.0)**: Job in avoided industry
- **Neutral (0.5)**: Unknown industry

#### Role Matching (Weight: 25%)
- **Perfect (1.0)**: Title matches preferred roles
- **Good (0.8)**: Seniority level matches
- **Avoided (0.0)**: Title contains avoided roles
- **Neutral (0.5)**: No match

#### Company Size Matching (Weight: 15%)
- **Perfect (1.0)**: Within preferred employee count range
- **Partial**: Proportional to how close it is to range
- **Bad (0.0)**: Far outside range

### Scoring Formula

Final job score includes preferences:

```
Overall Score = BaseScore + PreferencesBoost - Penalties

Where:
  BaseScore = 100 * (0.7*RequiredCoverage + 0.3*PreferredCoverage)
  
  PreferencesBoost (based on alignment):
    - Alignment >= 0.9: +15 points (max boost)
    - Alignment >= 0.75: +10.5 points
    - Alignment >= 0.6: +6 points
    - Alignment >= 0.5: +3 points
    - Alignment < 0.5: +0 points
  
  Penalties:
    - Missing required skills: -15 points each
    - Not remote (when wanted): -10 points
    - Seniority mismatch: -10 points
    - Salary below minimum: -10 points
    - Avoided industry: -10 points
    - Avoided role: -10 points
```

### Matching Pipeline Steps

1. **Extract** resume into Resume Evidence Units (REUs)
2. **Calculate Preferences Alignment** for each job:
   - Location match
   - Industry match
   - Company size match
   - Role/seniority match
3. **Match** REUs to Job Requirement Units via vector similarity
4. **Score** matches considering:
   - Required skills coverage (weight: 0.7)
   - Preferred skills coverage (weight: 0.3)
   - Preferences alignment boost (+0 to +15 points)
   - Penalties for mismatches
5. **Save** matches to database (only if score ≥ 30/100)

## Configuration Options

### In `config.yaml`:

```yaml
matching:
  enabled: true
  mode: "with_preferences"  # or "requirements_only"
  resume_file: "resume.json"
  preferences_file: "preferences.json"
  
  # Auto-invalidation settings
  invalidate_on_job_change: true    # Recalculate when job updates
  invalidate_on_resume_change: true # Recalculate when resume updates
  recalculate_existing: false       # Force recalculation even if match exists
  
  matcher:
    enabled: true
    similarity_threshold: 0.5       # Min similarity for match (0.0-1.0)
    batch_size: 100                 # Jobs to match per batch
    include_job_level_matching: true # Also match JD summary level
  
  scorer:
    enabled: true
    weight_required: 0.7            # Weight for required skills
    weight_preferred: 0.3           # Weight for preferred skills
    
    # Penalty amounts
    penalty_missing_required: 15.0
    penalty_location_mismatch: 10.0
    penalty_seniority_mismatch: 10.0
    penalty_compensation_mismatch: 10.0
    
    # Default preferences (used if preferences.json not provided)
    wants_remote: true
    min_salary: 5000000
    target_seniority: "mid"
```

## Example Workflow

```bash
# 1. Setup your files
cp preferences.example.json preferences.json
# Edit preferences.json

# 2. Update config
cat >> config.yaml << EOF
matching:
  enabled: true
  mode: "with_preferences"
  preferences_file: "preferences.json"
EOF

# 3. Run
python main.py
```

## Database Schema

Matches are stored in:
- `job_match` - Overall match scores and metadata
- `job_match_requirement` - Individual requirement-to-evidence mappings

Query your matches:
```sql
-- Top 10 matches with preferences info
SELECT 
  j.title,
  j.company,
  jm.overall_score,
  jm.base_score,
  jm.penalty_details->>'preferences_boost' as preferences_boost,
  jm.required_coverage,
  jm.penalties
FROM job_match jm
JOIN job_post j ON j.id = jm.job_post_id
WHERE jm.status = 'active'
ORDER BY jm.overall_score DESC
LIMIT 10;

-- Detailed requirements for a match
SELECT 
  jru.text as requirement,
  jmr.evidence_text as matched_evidence,
  jmr.similarity_score
FROM job_match_requirement jmr
JOIN job_requirement_unit jru ON jru.id = jmr.job_requirement_unit_id
WHERE jmr.job_match_id = 'YOUR_MATCH_ID';

-- Jobs that matched your preferences well
SELECT 
  j.title,
  j.company,
  jm.overall_score,
  jm.match_type
FROM job_match jm
JOIN job_post j ON j.id = jm.job_post_id
WHERE jm.match_type = 'with_preferences'
  AND jm.overall_score > 80
ORDER BY jm.overall_score DESC;
```

## Troubleshooting

**Q: My preferences file isn't being loaded**
- Check file path in `config.yaml`
- Ensure path is absolute or relative to working directory
- Verify JSON is valid: `python -m json.tool preferences.json`

**Q: Matches aren't using my preferences**
- Verify `mode: "with_preferences"` in config
- Check that `preferences_file` points to correct file
- Look for "Loaded preferences" in logs

**Q: No matches are being saved**
- Check minimum score threshold (default: 30/100)
- Verify jobs have been embedded (is_embedded = true)
- Check logs for "Saved X matches" message

**Q: Perfect preferences match not scoring highest**
- Check that job meets ALL your preferences
- Verify no penalties are being applied
- Review penalty_details in database for specific reasons

## Advanced Usage

### Custom Scoring Weights

Edit in `config.yaml` under `matching.scorer`:
```yaml
scorer:
  weight_required: 0.8      # Prioritize required skills
  weight_preferred: 0.2
  wants_remote: true
  penalty_location_mismatch: 20.0  # Stricter on location
```

### Multiple Resumes

You can run matching against different resumes:
```yaml
# In config.yaml
matching:
  resume_file: "resume_backend.json"  # or resume_frontend.json
```

Matches are keyed by resume fingerprint, so each resume gets its own set of matches.

### Programmatic Access

```python
from database.repository import JobRepository
from database.database import db_session_scope

with db_session_scope() as session:
    repo = JobRepository(session)
    matches = repo.get_matches_for_resume(
        resume_fingerprint="your_fp",
        min_score=70.0
    )
    for match in matches:
        print(f"{match.job.title}: {match.overall_score}")
        # Access preferences boost
        boost = match.penalty_details.get('preferences_boost', 0)
        print(f"  Preferences boost: +{boost}")
```

### Testing Your Preferences

Run the verification script:
```bash
python tests/verify_preferences.py
```

This will show you how different job characteristics score against your preferences.
