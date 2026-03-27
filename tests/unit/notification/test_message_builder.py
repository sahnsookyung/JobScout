#!/usr/bin/env python3
"""
Tests for notification message_builder module.

Tests cover:
1. JobNotificationContent model validation
2. NotificationMessageBuilder methods:
   - to_markdown(), to_html(), to_discord_embed()
   - build_batch_markdown(), build_batch_embeds()
   - Salary formatting, location formatting
   - Score color mapping
3. build_from_dict, build_from_orm methods
4. Apply section building
"""

import pytest
from unittest.mock import Mock

from notification.message_builder import (
    JobNotificationContent, JobInfo, MatchInfo, RequirementsInfo,
    NotificationMessageBuilder
)
from database.models import JobPost, JobMatch


def _make_mock_job_post(**kwargs):
    """Create a properly configured Mock JobPost with default values."""
    defaults = {
        'title': 'Test Position',
        'company': 'Test Company',
        'location_text': None,
        'is_remote': False,
        'work_from_home_type': None,
        'salary_min': None,
        'salary_max': None,
        'salary_interval': None,
        'job_type': None,
        'job_level': None,
        'description': None,
        'summary': None,
        'emails': [],
    }
    defaults.update(kwargs)
    mock = Mock(spec=JobPost)
    for key, value in defaults.items():
        setattr(mock, key, value)
    return mock


def _make_mock_job_match(**kwargs):
    """Create a properly configured Mock JobMatch with default values."""
    defaults = {
        'overall_score': 80.0,
        'fit_score': 75.0,
        'want_score': None,
        'required_coverage': 0.8,
        'total_requirements': 5,
        'matched_requirements_count': 4,
    }
    defaults.update(kwargs)
    mock = Mock(spec=JobMatch)
    for key, value in defaults.items():
        setattr(mock, key, value)
    return mock


class TestJobNotificationContent:
    """Test JobNotificationContent Pydantic model."""

    def test_job_notification_content_creation(self):
        """Test creating valid JobNotificationContent."""
        content = JobNotificationContent(
            job=JobInfo(
                title='Software Engineer',
                company='TechCorp',
                location='San Francisco, CA',
                is_remote=True
            ),
            match=MatchInfo(
                overall_score=85.0,
                fit_score=80.0,
                want_score=75.0,
                required_coverage=0.85
            ),
            requirements=RequirementsInfo(
                total=10,
                matched=8,
                key_matches=['Python', 'React']
            ),
            apply_url='https://example.com/apply'
        )

        assert content.job.title == 'Software Engineer'
        assert content.match.overall_score == 85.0
        assert content.requirements.matched == 8

    def test_job_notification_content_optional_fields(self):
        """Test JobNotificationContent with optional fields."""
        content = JobNotificationContent(
            job=JobInfo(
                title='Developer',
                company='Startup',
                is_remote=False
            ),
            match=MatchInfo(
                overall_score=70.0,
                fit_score=65.0,
                required_coverage=0.7
            ),
            requirements=RequirementsInfo(
                total=5,
                matched=3
            )
        )

        assert content.job.location is None
        assert content.job.salary is None
        assert content.match.want_score is None
        assert content.apply_url is None

    def test_job_notification_content_validation(self):
        """Test JobNotificationContent validates required fields."""
        # Missing required fields should raise validation error
        with pytest.raises(Exception):  # Pydantic ValidationError
            JobNotificationContent(
                job={},  # Missing required fields
                match={},
                requirements={}
            )


class TestNotificationMessageBuilderMarkdown:
    """Test markdown formatting methods."""

    def test_to_markdown_basic(self):
        """Test basic markdown formatting."""
        content = JobNotificationContent(
            job=JobInfo(
                title='Python Developer',
                company='TechCorp',
                location='Remote',
                is_remote=True,
                salary='$120k - $150k',
                job_type='Full-time',
                job_level='Mid-level'
            ),
            match=MatchInfo(
                overall_score=85.0,
                fit_score=80.0,
                want_score=75.0,
                required_coverage=0.85
            ),
            requirements=RequirementsInfo(
                total=10,
                matched=8,
                key_matches=['Python', 'FastAPI']
            ),
            apply_url='https://example.com/apply'
        )

        markdown = NotificationMessageBuilder.to_markdown(content)

        assert '🎯 **Python Developer**' in markdown
        assert '🏢 TechCorp' in markdown
        assert 'Remote' in markdown
        assert '💰 $120k - $150k' in markdown
        assert '📊 **85%** Match' in markdown
        assert 'Fit: 80%' in markdown
        assert 'Want: 75%' in markdown
        assert '✅ **8/10** requirements matched' in markdown
        assert '[Apply Here]' in markdown

    def test_to_markdown_no_want_score(self):
        """Test markdown without want score."""
        content = JobNotificationContent(
            job=JobInfo(
                title='Developer',
                company='Company',
                location='New York, NY',
                is_remote=False
            ),
            match=MatchInfo(
                overall_score=75.0,
                fit_score=70.0,
                want_score=None,
                required_coverage=0.75
            ),
            requirements=RequirementsInfo(total=5, matched=4)
        )

        markdown = NotificationMessageBuilder.to_markdown(content)

        assert 'Want:' not in markdown

    def test_to_markdown_truncates_long_description(self):
        """Test markdown truncates long descriptions."""
        long_desc = 'x' * 500
        content = JobNotificationContent(
            job=JobInfo(
                title='Developer',
                company='Company',
                location='Remote',
                is_remote=True,
                description=long_desc
            ),
            match=MatchInfo(
                overall_score=80.0,
                fit_score=75.0,
                required_coverage=0.8
            ),
            requirements=RequirementsInfo(total=5, matched=4)
        )

        markdown = NotificationMessageBuilder.to_markdown(content)

        # Should be truncated to ~280 chars + "..."
        assert '...' in markdown
        assert len([l for l in markdown.split('\n') if 'x' * 100 in l]) > 0

    def test_to_markdown_no_requirements(self):
        """Test markdown with zero requirements."""
        content = JobNotificationContent(
            job=JobInfo(
                title='Developer',
                company='Company',
                location='Remote',
                is_remote=True
            ),
            match=MatchInfo(
                overall_score=70.0,
                fit_score=65.0,
                required_coverage=0.7
            ),
            requirements=RequirementsInfo(total=0, matched=0)
        )

        markdown = NotificationMessageBuilder.to_markdown(content)

        assert '⚠️ **0/0** requirements matched' in markdown


class TestNotificationMessageBuilderHTML:
    """Test HTML formatting methods."""

    def test_to_html_basic(self):
        """Test basic HTML formatting."""
        content = JobNotificationContent(
            job=JobInfo(
                title='Python Developer',
                company='TechCorp',
                location='Remote',
                is_remote=True
            ),
            match=MatchInfo(
                overall_score=85.0,
                fit_score=80.0,
                required_coverage=0.85
            ),
            requirements=RequirementsInfo(total=10, matched=8)
        )

        html = NotificationMessageBuilder.to_html(content)

        assert '<b>' in html
        assert 'Python Developer' in html
        assert 'TechCorp' in html
        assert '<hr/>' in html

    def test_to_html_escaping(self):
        """Test HTML properly escapes special characters."""
        content = JobNotificationContent(
            job=JobInfo(
                title='Developer <Senior>',
                company='Tech & Co',
                location='Remote',
                is_remote=True
            ),
            match=MatchInfo(
                overall_score=80.0,
                fit_score=75.0,
                required_coverage=0.8
            ),
            requirements=RequirementsInfo(total=5, matched=4)
        )

        html = NotificationMessageBuilder.to_html(content)

        # Special chars should be escaped in markdown first
        assert 'Developer' in html


class TestNotificationMessageBuilderDiscord:
    """Test Discord embed formatting."""

    def test_to_discord_embed_basic(self):
        """Test basic Discord embed formatting."""
        content = JobNotificationContent(
            job=JobInfo(
                title='Python Developer',
                company='TechCorp',
                location='San Francisco, CA',
                is_remote=True,
                salary='$120k - $150k',
                job_type='Full-time'
            ),
            match=MatchInfo(
                overall_score=85.0,
                fit_score=80.0,
                required_coverage=0.85
            ),
            requirements=RequirementsInfo(total=10, matched=8)
        )

        embed = NotificationMessageBuilder.to_discord_embed(content)

        assert embed['title'] == '🎯 Python Developer'
        assert 'Match Score: **85%**' in embed['description']
        assert embed['color'] == 0x28A745  # Green for high score

        # Check fields
        field_names = [f['name'] for f in embed['fields']]
        assert '🏢 Company' in field_names
        assert '📍 Location' in field_names
        assert '💰 Salary' in field_names
        assert '📊 Match Score' in field_names

    def test_to_discord_embed_medium_score(self):
        """Test Discord embed color for medium score."""
        content = JobNotificationContent(
            job=JobInfo(title='Dev', company='Co', is_remote=True),
            match=MatchInfo(
                overall_score=65.0,
                fit_score=60.0,
                required_coverage=0.65
            ),
            requirements=RequirementsInfo(total=5, matched=3)
        )

        embed = NotificationMessageBuilder.to_discord_embed(content)

        assert embed['color'] == 0xFFC107  # Yellow for medium score

    def test_to_discord_embed_low_score(self):
        """Test Discord embed color for low score."""
        content = JobNotificationContent(
            job=JobInfo(title='Dev', company='Co', is_remote=True),
            match=MatchInfo(
                overall_score=35.0,
                fit_score=30.0,
                required_coverage=0.35
            ),
            requirements=RequirementsInfo(total=5, matched=2)
        )

        embed = NotificationMessageBuilder.to_discord_embed(content)

        assert embed['color'] == 0xDC3545  # Red for low score

    def test_to_discord_embed_truncates_description(self):
        """Test Discord embed truncates long descriptions."""
        long_desc = 'x' * 500
        content = JobNotificationContent(
            job=JobInfo(
                title='Dev',
                company='Co',
                is_remote=True,
                description=long_desc
            ),
            match=MatchInfo(
                overall_score=80.0,
                fit_score=75.0,
                required_coverage=0.8
            ),
            requirements=RequirementsInfo(total=5, matched=4)
        )

        embed = NotificationMessageBuilder.to_discord_embed(content)

        # Find description field
        desc_field = next((f for f in embed['fields'] if f['name'] == '📝 Description'), None)
        assert desc_field is not None
        assert len(desc_field['value']) <= 203  # 200 + "..."

    def test_to_discord_embed_no_apply_url(self):
        """Test Discord embed without apply URL."""
        content = JobNotificationContent(
            job=JobInfo(title='Dev', company='Co', location='Remote', is_remote=True),
            match=MatchInfo(
                overall_score=80.0,
                fit_score=75.0,
                required_coverage=0.8
            ),
            requirements=RequirementsInfo(total=5, matched=4),
            apply_url=None
        )

        embed = NotificationMessageBuilder.to_discord_embed(content)

        # Should not have Apply field
        field_names = [f['name'] for f in embed['fields']]
        assert '🔗 Apply' not in field_names


class TestNotificationMessageBuilderBatch:
    """Test batch notification formatting."""

    def test_build_batch_markdown_single(self):
        """Test batch markdown with single item."""
        content = JobNotificationContent(
            job=JobInfo(title='Dev', company='Co', location='Remote', is_remote=True),
            match=MatchInfo(
                overall_score=80.0,
                fit_score=75.0,
                required_coverage=0.8
            ),
            requirements=RequirementsInfo(total=5, matched=4)
        )

        markdown = NotificationMessageBuilder.build_batch_markdown([content])

        assert '🎯 **Dev**' in markdown
        assert '═' * 50 not in markdown  # No separator for single item

    def test_build_batch_markdown_multiple(self):
        """Test batch markdown with multiple items."""
        content1 = JobNotificationContent(
            job=JobInfo(title='Dev1', company='Co1', location='Remote', is_remote=True),
            match=MatchInfo(
                overall_score=80.0,
                fit_score=75.0,
                required_coverage=0.8
            ),
            requirements=RequirementsInfo(total=5, matched=4)
        )
        content2 = JobNotificationContent(
            job=JobInfo(title='Dev2', company='Co2', location='Remote', is_remote=True),
            match=MatchInfo(
                overall_score=85.0,
                fit_score=80.0,
                required_coverage=0.85
            ),
            requirements=RequirementsInfo(total=6, matched=5)
        )

        markdown = NotificationMessageBuilder.build_batch_markdown([content1, content2])

        assert '🎯 **Dev1**' in markdown
        assert '🎯 **Dev2**' in markdown
        assert '═' * 50 in markdown  # Separator between items

    def test_build_batch_embeds(self):
        """Test batch Discord embeds."""
        content1 = JobNotificationContent(
            job=JobInfo(title='Dev1', company='Co1', is_remote=True),
            match=MatchInfo(
                overall_score=80.0,
                fit_score=75.0,
                required_coverage=0.8
            ),
            requirements=RequirementsInfo(total=5, matched=4)
        )
        content2 = JobNotificationContent(
            job=JobInfo(title='Dev2', company='Co2', is_remote=True),
            match=MatchInfo(
                overall_score=85.0,
                fit_score=80.0,
                required_coverage=0.85
            ),
            requirements=RequirementsInfo(total=6, matched=5)
        )

        embeds = NotificationMessageBuilder.build_batch_embeds([content1, content2])

        assert len(embeds) == 2
        assert embeds[0]['title'] == '🎯 Dev1'
        assert embeds[1]['title'] == '🎯 Dev2'


class TestNotificationMessageBuilderHelpers:
    """Test helper methods."""

    def test_format_salary_full(self):
        """Test salary formatting with full range."""
        job_post = _make_mock_job_post(salary_min=100000, salary_max=150000, salary_interval='yearly')

        salary = NotificationMessageBuilder.format_salary(job_post)

        assert salary == '$100,000 - $150,000 yearly'

    def test_format_salary_min_only(self):
        """Test salary formatting with minimum only."""
        job_post = _make_mock_job_post(salary_min=100000, salary_max=None, salary_interval='yearly')

        salary = NotificationMessageBuilder.format_salary(job_post)

        assert salary == '$100,000+ yearly'

    def test_format_salary_max_only(self):
        """Test salary formatting with maximum only."""
        job_post = _make_mock_job_post(salary_min=None, salary_max=150000, salary_interval='hourly')

        salary = NotificationMessageBuilder.format_salary(job_post)

        assert salary == 'Up to $150,000 hourly'

    def test_format_salary_none(self):
        """Test salary formatting when no salary info."""
        job_post = _make_mock_job_post(salary_min=None, salary_max=None, salary_interval=None)

        salary = NotificationMessageBuilder.format_salary(job_post)

        assert salary is None

    def test_format_location_remote(self):
        """Test location formatting with remote."""
        job_post = _make_mock_job_post(location_text='San Francisco, CA', is_remote=True)

        location = NotificationMessageBuilder.format_location(job_post)

        assert 'San Francisco, CA' in location
        assert '🌐 Remote' in location

    def test_format_location_hybrid(self):
        """Test location formatting with hybrid."""
        job_post = _make_mock_job_post(location_text='New York, NY', is_remote=False, work_from_home_type='hybrid')

        location = NotificationMessageBuilder.format_location(job_post)

        assert 'New York, NY' in location
        assert '🌐 Hybrid' in location

    def test_format_location_office_only(self):
        """Test location formatting for office-only."""
        job_post = _make_mock_job_post(location_text='Chicago, IL', is_remote=False, work_from_home_type='office')

        location = NotificationMessageBuilder.format_location(job_post)

        assert 'Chicago, IL' in location
        assert 'Remote' not in location
        assert 'Hybrid' not in location

    def test_format_location_no_text(self):
        """Test location formatting when no location text."""
        job_post = _make_mock_job_post(location_text=None, is_remote=True)

        location = NotificationMessageBuilder.format_location(job_post)

        assert location == '🌐 Remote'

    def test_format_location_all_none(self):
        """Test location formatting when all None."""
        job_post = _make_mock_job_post(location_text=None, is_remote=False)

        location = NotificationMessageBuilder.format_location(job_post)

        assert location == '📍 Location not specified'

    def test_build_apply_section_with_url(self):
        """Test apply section with URL."""
        job_post = _make_mock_job_post(emails=[])

        apply_section = NotificationMessageBuilder.build_apply_section(
            'https://example.com/apply',
            job_post
        )

        assert '[Apply Here]' in apply_section
        assert 'https://example.com/apply' in apply_section

    def test_build_apply_section_with_email(self):
        """Test apply section with email."""
        job_post = _make_mock_job_post(emails=['jobs@company.com'])

        apply_section = NotificationMessageBuilder.build_apply_section(None, job_post)

        assert '📧 Apply' in apply_section
        assert 'jobs@company.com' in apply_section

    def test_build_apply_section_empty(self):
        """Test apply section with no URL or email."""
        job_post = _make_mock_job_post(emails=[])

        apply_section = NotificationMessageBuilder.build_apply_section(None, job_post)

        assert apply_section == ''


class TestBuildFromDict:
    """Test build_from_dict method."""

    def test_build_from_dict_complete(self):
        """Test building from complete dictionary."""
        data = {
            'job': {
                'title': 'Senior Developer',
                'company': 'TechCorp',
                'location': 'Remote',
                'is_remote': True,
                'salary': '$150k+',
                'job_type': 'Full-time',
                'job_level': 'Senior',
                'description': 'We are hiring...'
            },
            'match': {
                'overall_score': 90.0,
                'fit_score': 88.0,
                'want_score': 85.0,
                'required_coverage': 0.9
            },
            'requirements': {
                'total': 10,
                'matched': 9,
                'key_matches': ['Python', 'AWS']
            },
            'apply_url': 'https://example.com/apply'
        }

        content = NotificationMessageBuilder.build_from_dict(data)

        assert content.job.title == 'Senior Developer'
        assert content.match.overall_score == 90.0
        assert content.requirements.key_matches == ['Python', 'AWS']
        assert content.apply_url == 'https://example.com/apply'

    def test_build_from_dict_minimal(self):
        """Test building from minimal dictionary."""
        data = {
            'job': {
                'title': 'Developer',
                'company': 'Company',
                'is_remote': False
            },
            'match': {
                'overall_score': 70.0,
                'fit_score': 65.0,
                'required_coverage': 0.7
            },
            'requirements': {
                'total': 5,
                'matched': 3
            }
        }

        content = NotificationMessageBuilder.build_from_dict(data)

        assert content.job.title == 'Developer'
        assert content.match.want_score is None
        assert content.apply_url is None


class TestBuildFromORM:
    """Test build_from_orm method."""

    def test_build_from_orm_complete(self):
        """Test building from complete ORM objects."""
        job_post = _make_mock_job_post(
            title='Python Engineer',
            company='StartupXYZ',
            location_text='Boston, MA',
            is_remote=True,
            salary_min=120000,
            salary_max=160000,
            salary_interval='yearly',
            job_type='Full-time',
            job_level='Mid-level',
            description='Join our team...'
        )

        job_match = _make_mock_job_match(
            overall_score=88.0,
            fit_score=85.0,
            want_score=82.0,
            required_coverage=0.88,
            total_requirements=10,
            matched_requirements_count=9
        )

        content = NotificationMessageBuilder.build_from_orm(
            job_post, job_match,
            apply_url='https://example.com/apply'
        )

        assert content.job.title == 'Python Engineer'
        assert content.job.company == 'StartupXYZ'
        assert content.match.overall_score == 88.0
        assert content.match.want_score == 82.0
        assert content.apply_url == 'https://example.com/apply'

    def test_build_from_orm_null_scores(self):
        """Test building from ORM with null scores."""
        job_post = _make_mock_job_post()

        job_match = _make_mock_job_match(
            overall_score=None,
            fit_score=None,
            want_score=None,
            required_coverage=None,
            total_requirements=None,
            matched_requirements_count=None
        )

        content = NotificationMessageBuilder.build_from_orm(job_post, job_match)

        assert content.match.overall_score == 0.0
        assert content.match.fit_score == 0.0
        assert content.match.want_score is None

    def test_build_from_orm_missing_fields(self):
        """Test building from ORM with missing fields."""
        job_post = _make_mock_job_post(title=None, company=None)

        job_match = _make_mock_job_match(
            overall_score=75.0,
            fit_score=70.0,
            want_score=None,
            required_coverage=0.75,
            total_requirements=5,
            matched_requirements_count=4
        )

        content = NotificationMessageBuilder.build_from_orm(job_post, job_match)

        assert content.job.title == 'Unknown Position'
        assert content.job.company == 'Unknown Company'


class TestBuildNotificationContent:
    """Test build_notification_content method."""

    def test_build_notification_content(self):
        """Test building notification content from parameters."""
        job_post = _make_mock_job_post(
            title='Software Engineer',
            company='TechCorp',
            location_text='Seattle, WA',
            is_remote=True,
            salary_min=130000,
            salary_max=170000,
            salary_interval='yearly',
            job_type='Full-time',
            job_level='Senior',
            description='Description...'
        )

        content = NotificationMessageBuilder.build_notification_content(
            job_post=job_post,
            overall_score=92.0,
            fit_score=90.0,
            want_score=88.0,
            required_coverage=0.92,
            apply_url='https://example.com/apply'
        )

        assert content.job.title == 'Software Engineer'
        assert content.match.overall_score == 92.0
        assert content.match.want_score == 88.0
        assert content.apply_url == 'https://example.com/apply'


class TestScoreColorMapping:
    """Test score color mapping for Discord embeds."""

    def test_get_score_color_high(self):
        """Test color for high scores (>=80)."""
        assert NotificationMessageBuilder._get_score_color(80) == 0x28A745
        assert NotificationMessageBuilder._get_score_color(90) == 0x28A745
        assert NotificationMessageBuilder._get_score_color(100) == 0x28A745

    def test_get_score_color_medium(self):
        """Test color for medium scores (60-79)."""
        assert NotificationMessageBuilder._get_score_color(60) == 0xFFC107
        assert NotificationMessageBuilder._get_score_color(70) == 0xFFC107
        assert NotificationMessageBuilder._get_score_color(79) == 0xFFC107

    def test_get_score_color_low(self):
        """Test color for low scores (40-59)."""
        assert NotificationMessageBuilder._get_score_color(40) == 0xFD7E14
        assert NotificationMessageBuilder._get_score_color(50) == 0xFD7E14
        assert NotificationMessageBuilder._get_score_color(59) == 0xFD7E14

    def test_get_score_color_very_low(self):
        """Test color for very low scores (<40)."""
        assert NotificationMessageBuilder._get_score_color(0) == 0xDC3545
        assert NotificationMessageBuilder._get_score_color(20) == 0xDC3545
        assert NotificationMessageBuilder._get_score_color(39) == 0xDC3545
