"""Unit tests for etl/canonical_summary.py — branch coverage."""

from types import SimpleNamespace

from etl.canonical_summary import CanonicalJobSummaryGenerator


def _gen():
    return CanonicalJobSummaryGenerator()


def _job(**kw):
    defaults = dict(
        title="Engineer",
        company="Acme",
        location_text=None,
        work_from_home_type=None,
        is_remote=None,
        salary_min=None,
        salary_max=None,
        currency=None,
        company_description=None,
    )
    defaults.update(kw)
    return SimpleNamespace(**defaults)


class TestLimitBranches:
    def test_skips_empty_strings(self):
        result = CanonicalJobSummaryGenerator._limit(["", "python", ""], 10)
        assert result == ["python"]

    def test_deduplicates_case_insensitive(self):
        result = CanonicalJobSummaryGenerator._limit(["Python", "python", "Go"], 10)
        assert result == ["Python", "Go"]

    def test_stops_at_limit(self):
        result = CanonicalJobSummaryGenerator._limit(["a", "b", "c", "d"], 2)
        assert result == ["a", "b"]


class TestBuildRoleLine:
    def test_includes_seniority_when_present(self):
        job = _job(title="Engineer", company="Acme")
        line = CanonicalJobSummaryGenerator._build_role_line(job, {"seniority_level": "senior"})
        assert "seniority senior" in line

    def test_includes_summary_when_present(self):
        job = _job(title="Engineer", company="Acme")
        line = CanonicalJobSummaryGenerator._build_role_line(job, {"job_summary": "Build APIs"})
        assert "Build APIs" in line


class TestBuildWorkArrangementLine:
    def test_includes_remote_policy(self):
        job = _job()
        line = CanonicalJobSummaryGenerator._build_work_arrangement_line(job, {"remote_policy": "fully remote"})
        assert "fully remote" in line

    def test_includes_location(self):
        job = _job(location_text="Tokyo")
        line = CanonicalJobSummaryGenerator._build_work_arrangement_line(job, {})
        assert "location Tokyo" in line

    def test_includes_work_from_home_type(self):
        job = _job(work_from_home_type="hybrid")
        line = CanonicalJobSummaryGenerator._build_work_arrangement_line(job, {})
        assert "work from home hybrid" in line

    def test_appends_remote_possible_when_is_remote_true_and_not_mentioned(self):
        job = _job(is_remote=True)
        line = CanonicalJobSummaryGenerator._build_work_arrangement_line(job, {})
        assert "remote possible" in line


class TestBuildCompensationLine:
    def test_formats_salary_range_with_currency(self):
        job = _job()
        line = CanonicalJobSummaryGenerator._build_compensation_and_visa_line(
            job, {"salary_min": 80000, "salary_max": 120000, "currency": "USD"}
        )
        assert "USD 80000-120000" in line

    def test_formats_salary_range_unknown_bound(self):
        job = _job()
        line = CanonicalJobSummaryGenerator._build_compensation_and_visa_line(
            job, {"salary_min": None, "salary_max": 100000}
        )
        assert "?-100000" in line

    def test_visa_true(self):
        job = _job()
        line = CanonicalJobSummaryGenerator._build_compensation_and_visa_line(
            job, {"visa_sponsorship_available": True}
        )
        assert "visa sponsorship available" in line

    def test_visa_false(self):
        job = _job()
        line = CanonicalJobSummaryGenerator._build_compensation_and_visa_line(
            job, {"visa_sponsorship_available": False}
        )
        assert "visa sponsorship not indicated" in line

    def test_coerce_numeric_with_int(self):
        assert CanonicalJobSummaryGenerator._coerce_numeric(42) == 42.0

    def test_coerce_numeric_with_bool_returns_none(self):
        assert CanonicalJobSummaryGenerator._coerce_numeric(True) is None


class TestCompanyAndTeamCues:
    def test_includes_company_description(self):
        job = _job(company_description="We build great things")
        cues = CanonicalJobSummaryGenerator._company_and_team_cues(job, {})
        assert "We build great things" in cues

    def test_includes_tech_stack(self):
        job = _job()
        cues = CanonicalJobSummaryGenerator._company_and_team_cues(
            job, {"tech_stack": ["Python", "FastAPI"]}
        )
        assert any("Python" in c for c in cues)
