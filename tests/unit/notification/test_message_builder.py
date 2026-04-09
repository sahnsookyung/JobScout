"""Tests for notification.message_builder."""

from unittest.mock import Mock

import pytest

from database.models import JobMatch, JobPost
from notification.message_builder import (
    JobInfo,
    JobNotificationContent,
    MatchInfo,
    NotificationMessageBuilder,
    RequirementsInfo,
)


def _make_mock_job_post(**kwargs):
    defaults = {
        "title": "Test Position",
        "company": "Test Company",
        "location_text": None,
        "is_remote": False,
        "work_from_home_type": None,
        "salary_min": None,
        "salary_max": None,
        "salary_interval": None,
        "job_type": None,
        "job_level": None,
        "description": None,
        "summary": None,
        "emails": [],
    }
    defaults.update(kwargs)
    mock = Mock(spec=JobPost)
    for key, value in defaults.items():
        setattr(mock, key, value)
    return mock


def _make_mock_job_match(**kwargs):
    defaults = {
        "fit_score": 75.0,
        "required_coverage": 0.8,
        "total_requirements": 5,
        "matched_requirements_count": 4,
    }
    defaults.update(kwargs)
    mock = Mock(spec=JobMatch)
    for key, value in defaults.items():
        setattr(mock, key, value)
    return mock


def _content(**kwargs) -> JobNotificationContent:
    job = JobInfo(
        title="Python Developer",
        company="TechCorp",
        location="Remote",
        is_remote=True,
        salary="$120k - $150k",
        job_type="Full-time",
        job_level="Mid-level",
        description=kwargs.pop("description", None),
    )
    match = MatchInfo(
        fit_score=kwargs.pop("fit_score", 80.0),
        required_coverage=kwargs.pop("required_coverage", 0.85),
    )
    requirements = RequirementsInfo(
        total=kwargs.pop("total", 10),
        matched=kwargs.pop("matched", 8),
        key_matches=kwargs.pop("key_matches", ["Python", "FastAPI"]),
    )
    return JobNotificationContent(
        job=kwargs.pop("job", job),
        match=kwargs.pop("match", match),
        requirements=kwargs.pop("requirements", requirements),
        apply_url=kwargs.pop("apply_url", "https://example.com/apply"),
    )


def _content_with_ranking(**kwargs) -> JobNotificationContent:
    match = MatchInfo(
        fit_score=kwargs.pop("fit_score", 82.0),
        preference_score=kwargs.pop("preference_score", 0.74),
        required_coverage=kwargs.pop("required_coverage", 0.88),
        ranking_mode_used=kwargs.pop("ranking_mode_used", "preference_first"),
        explanation_label=kwargs.pop("explanation_label", "Preference alignment led this ranking"),
        dominant_reason_code=kwargs.pop("dominant_reason_code", "preference_alignment"),
    )
    return _content(match=match, **kwargs)


class TestJobNotificationContent:
    def test_creation(self):
        content = _content()
        assert content.job.title == "Python Developer"
        assert content.match.fit_score == 80.0
        assert content.requirements.matched == 8

    def test_validation(self):
        with pytest.raises(Exception):
            JobNotificationContent(job={}, match={}, requirements={})


class TestMarkdownFormatting:
    def test_to_markdown_basic(self):
        markdown = NotificationMessageBuilder.to_markdown(_content())

        assert "🎯 **Strong match**" in markdown
        assert "**Python Developer**" in markdown
        assert "🏢 TechCorp" in markdown
        assert "📊 Fit: **80%**" in markdown
        assert "🧩 Required coverage: 85%" in markdown
        assert "✅ **8/10** requirements matched" in markdown
        assert "[Apply Here]" in markdown

    def test_to_markdown_truncates_description(self):
        markdown = NotificationMessageBuilder.to_markdown(
            _content(description="x" * 500, apply_url=None)
        )
        assert "..." in markdown

    def test_to_markdown_zero_requirements(self):
        markdown = NotificationMessageBuilder.to_markdown(
            _content(total=0, matched=0, apply_url=None)
        )
        assert "⚠️ **0/0** requirements matched" in markdown

    def test_to_markdown_includes_preference_and_ranking_reason(self):
        markdown = NotificationMessageBuilder.to_markdown(_content_with_ranking())

        assert "🎯 **Matches your preferences**" in markdown
        assert "💡 Preference alignment: **74%**" in markdown
        assert "Why it surfaced: Preference alignment led this ranking" in markdown


class TestHtmlFormatting:
    def test_to_html_basic(self):
        html = NotificationMessageBuilder.to_html(_content(apply_url=None))
        assert "<b>" in html
        assert "Python Developer" in html
        assert "<hr/>" in html

    def test_to_html_converts_score_and_link_sections(self):
        html = NotificationMessageBuilder.to_html(_content_with_ranking())
        assert "<br/>📊 Fit: <b>82%</b>" in html
        assert "<br/>🔗 [Apply Here]" in html


class TestDiscordEmbedFormatting:
    def test_to_discord_embed_basic(self):
        embed = NotificationMessageBuilder.to_discord_embed(_content())
        field_names = [field["name"] for field in embed["fields"]]

        assert embed["title"] == "🎯 Strong match"
        assert "Python Developer at TechCorp" == embed["description"]
        assert embed["color"] == 0x28A745
        assert "🏢 Company" in field_names
        assert "📈 Required Coverage" in field_names

    def test_to_discord_embed_medium_color(self):
        embed = NotificationMessageBuilder.to_discord_embed(_content(fit_score=65.0))
        assert embed["color"] == 0xFFC107

    def test_to_discord_embed_includes_preference_and_ranking_reason(self):
        embed = NotificationMessageBuilder.to_discord_embed(_content_with_ranking())
        fields = {field["name"]: field["value"] for field in embed["fields"]}

        assert embed["title"] == "🎯 Matches your preferences"
        assert fields["💡 Preference Alignment"] == "74%"
        assert fields["🧭 Ranking Reason"] == "Preference alignment led this ranking"

    def test_to_discord_embed_low_color(self):
        embed = NotificationMessageBuilder.to_discord_embed(_content(fit_score=35.0))
        assert embed["color"] == 0xDC3545

    def test_to_discord_embed_truncates_description(self):
        embed = NotificationMessageBuilder.to_discord_embed(
            _content(description="x" * 500, apply_url=None)
        )
        description_field = next(
            field for field in embed["fields"] if field["name"] == "📝 Description"
        )
        assert len(description_field["value"]) <= 203


class TestBatchFormatting:
    def test_build_batch_markdown_single(self):
        markdown = NotificationMessageBuilder.build_batch_markdown([_content()])
        assert "🎯 **Strong match**" in markdown
        assert "═" * 50 not in markdown

    def test_build_batch_markdown_multiple(self):
        markdown = NotificationMessageBuilder.build_batch_markdown(
            [_content(), _content(job=JobInfo(title="Dev2", company="Co2", is_remote=True))]
        )
        assert "═" * 50 in markdown

    def test_build_batch_embeds(self):
        embeds = NotificationMessageBuilder.build_batch_embeds([_content(), _content()])
        assert len(embeds) == 2


class TestHelpers:
    def test_format_salary_full(self):
        salary = NotificationMessageBuilder.format_salary(
            _make_mock_job_post(salary_min=100000, salary_max=150000, salary_interval="yearly")
        )
        assert salary == "$100,000 - $150,000 yearly"

    def test_format_salary_min_only(self):
        salary = NotificationMessageBuilder.format_salary(
            _make_mock_job_post(salary_min=100000, salary_interval="yearly")
        )
        assert salary == "$100,000+ yearly"

    def test_format_salary_max_only(self):
        salary = NotificationMessageBuilder.format_salary(
            _make_mock_job_post(salary_max=150000, salary_interval="hourly")
        )
        assert salary == "Up to $150,000 hourly"

    def test_format_location_remote(self):
        location = NotificationMessageBuilder.format_location(
            _make_mock_job_post(location_text="San Francisco, CA", is_remote=True)
        )
        assert "🌐 Remote" in location

    def test_build_apply_section_with_url(self):
        apply = NotificationMessageBuilder.build_apply_section(
            "https://example.com/apply",
            _make_mock_job_post(),
        )
        assert "[Apply Here]" in apply

    def test_build_apply_section_with_email(self):
        apply = NotificationMessageBuilder.build_apply_section(
            None,
            _make_mock_job_post(emails=["jobs@company.com"]),
        )
        assert "jobs@company.com" in apply


class TestBuildFromDict:
    def test_complete(self):
        content = NotificationMessageBuilder.build_from_dict(
            {
                "job": {
                    "title": "Senior Developer",
                    "company": "TechCorp",
                    "location": "Remote",
                    "is_remote": True,
                },
                "match": {
                    "fit_score": 88.0,
                    "required_coverage": 0.9,
                },
                "requirements": {
                    "total": 10,
                    "matched": 9,
                    "key_matches": ["Python", "AWS"],
                },
                "apply_url": "https://example.com/apply",
            }
        )
        assert content.job.title == "Senior Developer"
        assert content.match.fit_score == 88.0
        assert content.requirements.key_matches == ["Python", "AWS"]

    def test_minimal(self):
        content = NotificationMessageBuilder.build_from_dict(
            {
                "job": {"title": "Developer", "company": "Company", "is_remote": False},
                "match": {"fit_score": 65.0, "required_coverage": 0.7},
                "requirements": {"total": 5, "matched": 3},
            }
        )
        assert content.job.title == "Developer"
        assert content.apply_url is None


class TestBuildFromOrm:
    def test_complete(self):
        content = NotificationMessageBuilder.build_from_orm(
            _make_mock_job_post(
                title="Python Engineer",
                company="StartupXYZ",
                location_text="Boston, MA",
                is_remote=True,
                salary_min=120000,
                salary_max=160000,
                salary_interval="yearly",
                job_type="Full-time",
                job_level="Mid-level",
                description="Join our team...",
            ),
            _make_mock_job_match(
                fit_score=88.0,
                required_coverage=0.88,
                total_requirements=10,
                matched_requirements_count=9,
            ),
            apply_url="https://example.com/apply",
        )
        assert content.job.title == "Python Engineer"
        assert content.match.fit_score == 88.0
        assert content.apply_url == "https://example.com/apply"

    def test_null_scores(self):
        content = NotificationMessageBuilder.build_from_orm(
            _make_mock_job_post(),
            _make_mock_job_match(
                fit_score=None,
                required_coverage=None,
                total_requirements=None,
                matched_requirements_count=None,
            ),
        )
        assert content.match.fit_score == 0.0
        assert content.match.required_coverage == 0.0


class TestBuildNotificationContent:
    def test_build_notification_content(self):
        content = NotificationMessageBuilder.build_notification_content(
            job_post=_make_mock_job_post(
                title="Software Engineer",
                company="TechCorp",
                location_text="Seattle, WA",
                is_remote=True,
            ),
            fit_score=90.0,
            required_coverage=0.92,
            apply_url="https://example.com/apply",
        )
        assert content.job.title == "Software Engineer"
        assert content.match.fit_score == 90.0
        assert content.apply_url == "https://example.com/apply"


class TestScoreColorMapping:
    def test_get_score_color(self):
        assert NotificationMessageBuilder._get_score_color(80) == 0x28A745
        assert NotificationMessageBuilder._get_score_color(60) == 0xFFC107
        assert NotificationMessageBuilder._get_score_color(40) == 0xFD7E14
        assert NotificationMessageBuilder._get_score_color(39) == 0xDC3545
