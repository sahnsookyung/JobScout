# SonarQube Issue Fix Plan

## Current Status

- **Total Issues:** 430 unresolved issues
- **Branch:** `refactor-microservices` (now includes latest `main` with `refactor-compact-controls`)
- **Coverage:** Not yet uploaded to SonarCloud (fix in progress)

---

## Issue Breakdown by Priority

### 🔴 BLOCKER (3 issues)
| Rule | Count | Issue |
|------|-------|-------|
| `python:S2068` | 17 | Hardcoded "password" credentials |
| `python:S8410` | 14 | Missing `Annotated` type hints for FastAPI DI |
| `python:S6437` | 1 | Compromised password to revoke |

### 🟠 CRITICAL (28 issues)
| Rule | Count | Issue |
|------|-------|-------|
| `python:S3776` | 28 | Cognitive Complexity too high (functions need refactoring) |
| `python:S8396` | 25 | Pydantic fields missing explicit default values |
| `python:S1192` | 10 | Duplicated string literals (should be constants) |
| `typescript:S2004` | 5 | Nested functions > 4 levels deep |
| `python:S5754` | 5 | Bare `except:` without exception type |
| `python:S5727` | 3 | Identity check always True |
| `shelldre:S131` | 1 | Missing default case in match statement |

### 🟡 MAJOR (200+ issues)
| Rule | Count | Issue |
|------|-------|-------|
| `shelldre:S7688` | 86 | Use `[[` instead of `[` in shell scripts |
| `python:S3457` | 44 | F-strings without placeholders |
| `shelldre:S7682` | 29 | Missing explicit return in shell functions |
| `shelldre:S7679` | 19 | Shell positional parameters not assigned to variables |
| `python:S2068` | 17 | Hardcoded credentials |
| `python:S1172` | 14 | Unused function parameters |
| `python:S8415` | 9 | Undocumented HTTPException status codes |
| `typescript:S6759` | 9 | Component props not marked readonly |
| `python:S6711` | 8 | Legacy `numpy.random` usage |
| `python:S1244` | 7 | Floating point equality checks |
| `typescript:S3358` | 5 | Nested ternary operations |
| `python:S125` | 3 | Commented-out code |
| `shelldre:S7677` | 3 | Shell error messages not redirected to stderr |
| `typescript:S6848` | 3 | Non-native interactive elements |

### ⚪ MINOR (50+ issues)
| Rule | Count | Issue |
|------|-------|-------|
| `python:S1481` | 25 | Unused local variables (should use `_`) |
| `typescript:S6767` | 6 | Unused PropTypes |
| `python:S7498` | 8 | Literal dict construction instead of `{}` |
| `typescript:S1128` | 4 | Unused imports |
| `python:S7503` | 4 | Unnecessary `async` functions |
| `typescript:S1082` | 2 | Non-interactive elements with click handlers |

---

## Files Requiring Most Attention

| Issues | File |
|--------|------|
| 54 | `scripts/setup_local_env/start.sh` |
| 42 | `scripts/setup_local_env/stop.sh` |
| 25 | `scripts/setup_local_env/logs.sh` |
| 24 | `web/backend/models/responses.py` |
| 19 | `scripts/validate_setup.sh` |
| 17 | `tests/integration/test_openai_schema_validation.py` |
| 16 | `tests/unit/core/scorer/test_fit_want_scoring.py` |
| 15 | `web/frontend/src/features/matches/components/MatchDetailsModal.tsx` |
| 14 | `web/backend/routers/pipeline.py` |
| 14 | `web/frontend/src/utils/indexedDB.ts` |

---

## Fix Strategy (Phased Approach)

### Phase 1: Security & Critical Issues (BLOCKER + CRITICAL)
**Priority:** 🔴 Highest - These can cause security vulnerabilities or bugs

1. **Hardcoded Credentials** (17 issues - BLOCKER)
   - Replace hardcoded passwords with environment variables
   - Files: Test files, scripts
   - Action: Use `os.getenv()` or `.env` files

2. **FastAPI Type Hints** (14 issues - BLOCKER)
   - Add `Annotated` type hints for dependency injection
   - Files: `web/backend/routers/*.py`
   - Action: Update to FastAPI modern style

3. **Cognitive Complexity** (28 issues - CRITICAL)
   - Refactor complex functions (reduce nesting, extract methods)
   - Files: `main.py`, test files, `profiler.py`
   - Action: Break down large functions

4. **Pydantic Field Defaults** (25 issues - CRITICAL)
   - Add explicit `default=` or `default_factory=` to Optional fields
   - Files: `web/backend/models/responses.py`
   - Action: Update Pydantic model definitions

5. **Exception Handling** (5 issues - CRITICAL)
   - Replace bare `except:` with specific exception types
   - Action: Use `except Exception:` or specific types

### Phase 2: Shell Script Improvements (MAJOR)
**Priority:** 🟠 High - Shell scripts have 150+ issues

1. **Modern Bash Syntax** (86 issues)
   - Replace `[` with `[[` for conditionals
   - Files: All shell scripts in `scripts/`

2. **Return Statements** (29 issues)
   - Add explicit `return 0` at end of functions
   - Files: `start.sh`, `stop.sh`, `logs.sh`

3. **Positional Parameters** (19 issues)
   - Assign `$1`, `$2` to named variables at function start
   - Action: `local param_name="$1"`

4. **Error Redirection** (3 issues)
   - Redirect error messages to stderr: `>&2`
   - Action: `echo "Error" >&2`

### Phase 3: Python Code Quality (MAJOR)
**Priority:** 🟡 Medium - Improves maintainability

1. **F-string Cleanup** (44 issues)
   - Remove unnecessary `f` prefix from strings without placeholders
   - Action: Simple find/replace

2. **Unused Parameters** (14 issues)
   - Remove or rename to `_` for intentionally unused
   - Action: Remove or use `_param`

3. **HTTPException Documentation** (9 issues)
   - Add `responses={400: ...}` to FastAPI decorators
   - Files: `web/backend/routers/pipeline.py`

4. **Duplicated Literals** (10 issues)
   - Extract to module-level constants
   - Action: `TIMEOUT = 30` at top of file

5. **Floating Point Comparisons** (7 issues)
   - Use `math.isclose()` or epsilon comparisons
   - Action: `abs(a - b) < 1e-9`

### Phase 4: TypeScript/React Issues (MAJOR + MINOR)
**Priority:** 🟡 Medium - Frontend code quality

1. **Readonly Props** (9 issues)
   - Mark component props as `readonly`
   - Files: `.tsx` components

2. **Nested Ternary** (5 issues)
   - Extract to separate variables or if/else
   - Files: `MatchDetailsModal.tsx`

3. **Unused Imports/Props** (10 issues)
   - Remove unused imports and PropTypes
   - Action: Tree-shake unused code

4. **Accessibility** (6 issues)
   - Fix form labels, interactive elements
   - Action: Proper ARIA attributes

### Phase 5: Cleanup & Minor Issues (MINOR)
**Priority:** ⚪ Low - Nice to have

1. **Commented-out Code** (3 issues)
   - Remove or document why it's commented
   - Action: Delete or add explanation

2. **Unused Variables** (25 issues)
   - Replace with `_` or remove
   - Action: `del _` or rename

3. **Async Cleanup** (4 issues)
   - Remove unnecessary `async` from sync functions
   - Action: Make synchronous

---

## Recommended Order of Execution

```
Week 1: Phase 1 (Security & Critical)
  - Day 1-2: Hardcoded credentials (17 issues)
  - Day 3: FastAPI type hints (14 issues)
  - Day 4-5: Cognitive complexity (28 issues)

Week 2: Phase 1 continued + Phase 2
  - Day 1-2: Pydantic defaults + Exception handling
  - Day 3-5: Shell script improvements (150+ issues)

Week 3: Phase 3 (Python Quality)
  - F-strings, unused params, HTTPException docs
  - Duplicated literals, float comparisons

Week 4: Phase 4 + 5 (TypeScript + Cleanup)
  - TypeScript readonly, accessibility
  - Minor cleanup issues
```

---

## Quick Wins (Low Effort, High Impact)

These can be fixed in bulk with simple find/replace:

1. ✅ **F-strings without placeholders** (44 issues) - Regex: `f"([^{}]+)"` → `"\1"`
2. ✅ **Shell `[[` syntax** (86 issues) - Regex: `if \[` → `if [[`
3. ✅ **Unused variables** (25 issues) - Rename to `_`
4. ✅ **Commented code** (3 issues) - Delete

---

## Coverage Upload Status

**Current:** Coverage files not being uploaded to SonarCloud

**Fixed:** Updated `.github/workflows/sonarqube.yml` to:
- Verify `coverage.xml` exists before scan
- Proper ordering of test → scan → upload

**Next:** Push changes and verify coverage appears in SonarCloud

---

## Success Metrics

After fixes complete:
- [ ] 0 BLOCKER issues
- [ ] 0 CRITICAL issues
- [ ] < 50 MAJOR issues
- [ ] Coverage ≥ 80% on new code
- [ ] Quality Gate passes

---

## Notes

- **Do not fix test file issues** unless they indicate real problems
- **Shell scripts** are development utilities (lower priority than production code)
- **Focus on production code first:** `web/backend/`, `services/`, `core/`, `etl/`
- Some issues may be **false positives** - mark as "Won't Fix" in SonarCloud with justification
