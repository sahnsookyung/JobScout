# SonarQube Integration

## Overview

JobScout uses **SonarCloud** for continuous code quality analysis with automatic coverage reporting.

## Configuration

### GitHub Secrets (Already Configured)

- `SONAR_TOKEN` - SonarCloud authentication token ✅
- `SONAR_HOST_URL` - `https://sonarcloud.io` ✅

### Files

- `.github/workflows/sonarqube.yml` - CI/CD workflow
- `sonar-project.properties` - SonarQube configuration

## What Gets Analyzed

### On Every Push/PR

1. **Run all tests with coverage**
2. **Generate coverage.xml** (Cobertura format)
3. **Upload to SonarCloud**
4. **Quality gate check**

### Coverage Reports

**Yes, you get local coverage reports even with SonarCloud!**

The workflow generates:
- `coverage.xml` - Machine-readable coverage data
- `htmlcov/` - Human-readable HTML report

**Access coverage reports:**
1. Go to GitHub Actions
2. Click on the SonarQube workflow run
3. Scroll to "Artifacts" section
4. Download `coverage-xml` or `test-results`
5. Open `htmlcov/index.html` in browser

## Running Locally

### With Coverage

```bash
# Run tests with coverage
uv run pytest tests/ \
  --cov=services \
  --cov=core \
  --cov=database \
  --cov=etl \
  --cov=pipeline \
  --cov=notification \
  --cov=web \
  --cov=modal \
  --cov-report=html \
  --cov-report=term \
  -v
```

### View Coverage

```bash
# Open HTML report
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

## SonarCloud Project

**URL:** https://sonarcloud.io/project/overview?id=jobscout

### Quality Gates

SonarCloud checks:
- ✅ **Bugs:** 0 bugs
- ✅ **Vulnerabilities:** 0 vulnerabilities
- ✅ **Security Hotspots:** Reviewed
- ✅ **Coverage:** ≥ 80% on new code
- ✅ **Duplications:** ≤ 3%

## Troubleshooting

### Coverage Not Showing in SonarCloud

1. Check workflow ran successfully
2. Verify `coverage.xml` was generated
3. Check `sonar-project.properties` has correct path:
   ```properties
   sonar.python.coverage.reportPaths=coverage.xml
   ```

### Quality Gate Fails

1. Click "Quality Gates" in SonarCloud
2. See which metric failed
3. Fix issues in code
4. Push fix - automatic re-analysis

## Resources

- [SonarCloud Documentation](https://docs.sonarcloud.io/)
- [pytest-cov Documentation](https://pytest-cov.readthedocs.io/)
