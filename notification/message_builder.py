import html
import re
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse
from database.models import JobMatch, JobPost
from pydantic import BaseModel


class JobNotificationContent(BaseModel):
    job: "JobInfo"
    match: "MatchInfo"
    requirements: "RequirementsInfo"
    apply_url: Optional[str] = None


class JobInfo(BaseModel):
    title: str
    company: str
    location: Optional[str] = None
    is_remote: bool
    salary: Optional[str] = None
    job_type: Optional[str] = None
    job_level: Optional[str] = None
    description: Optional[str] = None


class MatchInfo(BaseModel):
    fit_score: float
    preference_score: Optional[float] = None
    required_coverage: float
    ranking_mode_used: Optional[str] = None
    explanation_label: Optional[str] = None
    dominant_reason_code: Optional[str] = None


class RequirementsInfo(BaseModel):
    total: int
    matched: int
    key_matches: List[str] = []


JobNotificationContent.model_rebuild()


class NotificationMessageBuilder:
    @staticmethod
    def _safe_link_url(value: Optional[str]) -> Optional[str]:
        raw = str(value or "").strip()
        if not raw:
            return None
        parsed = urlparse(raw)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return None
        return raw

    @staticmethod
    def _safe_optional_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _headline_for_match(match_info: "MatchInfo") -> str:
        mode = (match_info.ranking_mode_used or "").strip().lower()
        if mode == "fit_first":
            return "Strong fit match"
        if mode == "preference_first":
            return "Matches your preferences"
        if mode == "balanced":
            return "Balanced fit and preference match"
        return "Strong match"

    @staticmethod
    def build_apply_section(apply_url: Optional[str], job_post: JobPost) -> str:
        """Build apply link section."""
        safe_apply_url = NotificationMessageBuilder._safe_link_url(apply_url)
        if safe_apply_url:
            return f"🔗 [Apply Here]({safe_apply_url})"
        
        if hasattr(job_post, 'emails') and job_post.emails:
            emails = job_post.emails if isinstance(job_post.emails, list) else [job_post.emails]
            if emails:
                return f"📧 Apply: {emails[0]}"
        
        return ""
    
    @staticmethod
    def format_salary(job_post: JobPost) -> Optional[str]:
        """Format salary information."""
        salary_min = getattr(job_post, 'salary_min', None)
        salary_max = getattr(job_post, 'salary_max', None)
        interval = getattr(job_post, 'salary_interval', '') or ''
        
        if salary_min and salary_max:
            return f"${salary_min:,.0f} - ${salary_max:,.0f} {interval}".strip()
        elif salary_min:
            return f"${salary_min:,.0f}+ {interval}".strip()
        elif salary_max:
            return f"Up to ${salary_max:,.0f} {interval}".strip()
        return None
    
    @staticmethod
    def format_location(job_post: JobPost) -> str:
        """Format location with remote indicator."""
        parts = []
        
        location_text = getattr(job_post, 'location_text', None)
        if location_text:
            parts.append(location_text)
        
        is_remote = getattr(job_post, 'is_remote', False)
        if is_remote:
            parts.append("🌐 Remote")
        else:
            wfh_type = getattr(job_post, 'work_from_home_type', None)
            if wfh_type and wfh_type != "office":
                parts.append("🌐 Hybrid")
        
        return " | ".join(parts) if parts else "📍 Location not specified"
    
    @staticmethod
    def _job_info_from_orm(job_post: JobPost) -> "JobInfo":
        """Build a JobInfo from an ORM JobPost object."""
        return JobInfo(
            title=getattr(job_post, 'title', None) or "Unknown Position",
            company=getattr(job_post, 'company', None) or "Unknown Company",
            location=NotificationMessageBuilder.format_location(job_post),
            is_remote=getattr(job_post, 'is_remote', False) or False,
            salary=NotificationMessageBuilder.format_salary(job_post),
            job_type=getattr(job_post, 'job_type', None),
            job_level=getattr(job_post, 'job_level', None),
            description=getattr(job_post, 'description', None) or getattr(job_post, 'summary', None),
        )

    @staticmethod
    def build_notification_content(
        job_post: JobPost,
        fit_score: float,
        required_coverage: float = 0,
        apply_url: Optional[str] = None,
        preference_score: Optional[float] = None,
        ranking_snapshot: Optional[Dict[str, Any]] = None,
    ) -> JobNotificationContent:
        """Build notification content from individual parameters."""

        job_info = NotificationMessageBuilder._job_info_from_orm(job_post)
        ranking_snapshot = ranking_snapshot or {}
        
        match_info = MatchInfo(
            fit_score=float(fit_score),
            preference_score=(
                None if preference_score is None else float(preference_score)
            ),
            required_coverage=float(required_coverage),
            ranking_mode_used=ranking_snapshot.get("ranking_mode_used"),
            explanation_label=ranking_snapshot.get("explanation_label"),
            dominant_reason_code=ranking_snapshot.get("dominant_reason_code"),
        )
        
        return JobNotificationContent(
            job=job_info,
            match=match_info,
            requirements=RequirementsInfo(total=0, matched=0, key_matches=[]),
            apply_url=apply_url,
        )
    
    @staticmethod
    def build_from_dict(data: Dict[str, Any]) -> JobNotificationContent:
        """Build notification content from a dictionary (for API/metadata)."""
        job_data = data.get('job', {})
        match_data = data.get('match', {})
        req_data = data.get('requirements', {})
        
        job_info = JobInfo(
            title=job_data.get('title', 'Unknown Position'),
            company=job_data.get('company', 'Unknown'),
            location=job_data.get('location'),
            is_remote=job_data.get('is_remote', False),
            salary=job_data.get('salary'),
            job_type=job_data.get('job_type'),
            job_level=job_data.get('job_level'),
            description=job_data.get('description'),
        )
        
        match_info = MatchInfo(
            fit_score=match_data.get('fit_score', 0),
            preference_score=match_data.get('preference_score'),
            required_coverage=match_data.get('required_coverage', 0),
            ranking_mode_used=match_data.get('ranking_mode_used'),
            explanation_label=match_data.get('explanation_label'),
            dominant_reason_code=match_data.get('dominant_reason_code'),
        )
        
        requirements_info = RequirementsInfo(
            total=req_data.get('total', 0),
            matched=req_data.get('matched', 0),
            key_matches=req_data.get('key_matches', []),
        )
        
        return JobNotificationContent(
            job=job_info,
            match=match_info,
            requirements=requirements_info,
            apply_url=data.get('apply_url'),
        )
    
    @staticmethod
    def build_from_orm(
        job_post: JobPost,
        job_match: JobMatch,
        apply_url: Optional[str] = None
    ) -> JobNotificationContent:
        """Build notification content from ORM objects (for pipeline integration).
        
        This method extracts data directly from SQLAlchemy ORM objects,
        handling Column types that need to be accessed via getattr.
        """
        job_info = NotificationMessageBuilder._job_info_from_orm(job_post)
        ranking_snapshot = getattr(job_match, 'ranking_snapshot', {}) or {}
        if not isinstance(ranking_snapshot, dict):
            ranking_snapshot = {}
        preference_score = NotificationMessageBuilder._safe_optional_float(
            getattr(job_match, 'preference_score', None)
        )

        match_info = MatchInfo(
            fit_score=float(getattr(job_match, 'fit_score', 0) or 0),
            preference_score=preference_score,
            required_coverage=float(getattr(job_match, 'required_coverage', 0) or 0),
            ranking_mode_used=ranking_snapshot.get('ranking_mode_used'),
            explanation_label=ranking_snapshot.get('explanation_label'),
            dominant_reason_code=ranking_snapshot.get('dominant_reason_code'),
        )
        
        return JobNotificationContent(
            job=job_info,
            match=match_info,
            requirements=RequirementsInfo(
                total=getattr(job_match, 'total_requirements', 0) or 0,
                matched=getattr(job_match, 'matched_requirements_count', 0) or 0,
                key_matches=[],
            ),
            apply_url=apply_url,
        )
    
    @staticmethod
    def to_markdown(content: JobNotificationContent) -> str:
        """Convert notification content to markdown format."""
        lines = []
        headline = NotificationMessageBuilder._headline_for_match(content.match)
        
        lines.append(f"🎯 **{headline}**")
        lines.append(f"**{content.job.title}**")
        lines.append(f"🏢 {content.job.company}")
        lines.append(content.job.location or "📍 Location not specified")
        
        if content.job.salary:
            lines.append(f"💰 {content.job.salary}")
        
        details = []
        if content.job.job_type:
            details.append(content.job.job_type)
        if content.job.job_level:
            details.append(content.job.job_level)
        if details:
            lines.append(" | ".join(details))
        
        lines.append("")
        lines.append("─" * 40)
        lines.append("")
        
        coverage = content.match.required_coverage * 100
        score_lines = [f"📊 Fit: **{content.match.fit_score:.0f}%**"]
        if content.match.preference_score is not None:
            score_lines.append(
                f"💡 Preference alignment: **{content.match.preference_score * 100:.0f}%**"
            )
        score_lines.append(f"🧩 Required coverage: {coverage:.0f}%")
        lines.append("\n".join(score_lines))

        if content.match.explanation_label:
            lines.append("")
            lines.append(f"Why it surfaced: {content.match.explanation_label}")
        
        lines.append("")
        req_matched = content.requirements.matched
        req_total = content.requirements.total
        if req_total > 0:
            lines.append(f"✅ **{req_matched}/{req_total}** requirements matched")
        else:
            lines.append(f"⚠️ **{req_matched}/{req_total}** requirements matched")
        
        safe_apply_url = NotificationMessageBuilder._safe_link_url(content.apply_url)
        if safe_apply_url:
            lines.append("")
            lines.append(f"🔗 [Apply Here]({safe_apply_url})")

        if content.job.description:
            lines.append("")
            lines.append("📝 **Description**")
            # Truncate if too long (approx 280 chars)
            desc = content.job.description
            if len(desc) > 280:
                desc = desc[:280] + "..."
            lines.append(desc)
        
        lines.append("")
        lines.append("🔍 View details: /api/matches")
        
        return "\n".join(lines)
    
    @staticmethod
    def to_html(content: JobNotificationContent) -> str:
        """Convert notification content to HTML format (for Telegram/Email)."""
        markdown = NotificationMessageBuilder.to_markdown(content)

        rendered = html.escape(markdown, quote=False)
        rendered = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', rendered)
        rendered = rendered.replace("─" * 40, "<hr/>")
        rendered = rendered.replace("\n", "<br/>")

        return rendered
    
    @staticmethod
    def to_discord_embed(content: JobNotificationContent) -> Dict[str, Any]:
        """Convert notification content to Discord embed format."""
        coverage = content.match.required_coverage * 100
        
        fields = [
            {"name": "🏢 Company", "value": content.job.company, "inline": True},
            {"name": "📍 Location", "value": content.job.location, "inline": True},
        ]
        
        if content.job.salary:
            fields.append({"name": "💰 Salary", "value": content.job.salary, "inline": True})
        
        if content.job.job_type:
            fields.append({"name": "📋 Type", "value": content.job.job_type, "inline": True})
        
        fields.extend([
            {"name": "🎯 Fit Score", "value": f"**{content.match.fit_score:.0f}%**", "inline": True},
            {"name": "📈 Required Coverage", "value": f"{coverage:.0f}%", "inline": True},
        ])
        if content.match.preference_score is not None:
            fields.append({
                "name": "💡 Preference Alignment",
                "value": f"{content.match.preference_score * 100:.0f}%",
                "inline": True,
            })
        if content.match.explanation_label:
            fields.append({
                "name": "🧭 Ranking Reason",
                "value": content.match.explanation_label,
                "inline": False,
            })
        
        req_matched = content.requirements.matched
        req_total = content.requirements.total
        if req_total > 0:
            fields.append({
                "name": "✅ Requirements",
                "value": f"{req_matched}/{req_total} matched",
                "inline": False
            })

        if content.job.description:
            desc = content.job.description
            if len(desc) > 200:
                desc = desc[:200] + "..."
            fields.append({
                "name": "📝 Description",
                "value": desc,
                "inline": False
            })
        
        safe_apply_url = NotificationMessageBuilder._safe_link_url(content.apply_url)
        if safe_apply_url:
            fields.append({
                "name": "🔗 Apply",
                "value": f"[Apply Here]({safe_apply_url})",
                "inline": True
            })
        
        color = NotificationMessageBuilder._get_score_color(content.match.fit_score)
        
        return {
            "title": f"🎯 {NotificationMessageBuilder._headline_for_match(content.match)}",
            "description": f"{content.job.title} at {content.job.company}",
            "color": color,
            "fields": fields,
            "footer": {"text": "JobScout Notifications"},
            "timestamp": None,
        }
    
    @staticmethod
    def _get_score_color(score: float) -> int:
        """Get Discord embed color based on score."""
        if score >= 80:
            return 0x28A745
        elif score >= 60:
            return 0xFFC107
        elif score >= 40:
            return 0xFD7E14
        return 0xDC3545
    
    @staticmethod
    def build_batch_markdown(contents: List[JobNotificationContent]) -> str:
        """Build markdown for multiple job notifications with separators."""
        if len(contents) == 1:
            return NotificationMessageBuilder.to_markdown(contents[0])
        
        parts = []
        for i, content in enumerate(contents):
            if i > 0:
                parts.append("")
                parts.append("═" * 50)
                parts.append("")
            parts.append(NotificationMessageBuilder.to_markdown(content))
        
        return "\n".join(parts)
    
    @staticmethod
    def build_batch_embeds(contents: List[JobNotificationContent]) -> List[Dict[str, Any]]:
        """Build list of Discord embeds for multiple jobs."""
        return [NotificationMessageBuilder.to_discord_embed(c) for c in contents]
