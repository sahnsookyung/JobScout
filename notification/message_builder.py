import re
from typing import List, Optional, Dict, Any
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
    location: Optional[str]
    is_remote: bool
    salary: Optional[str] = None
    job_type: Optional[str] = None
    job_level: Optional[str] = None


class MatchInfo(BaseModel):
    overall_score: float
    fit_score: float
    want_score: Optional[float] = None
    required_coverage: float


class RequirementsInfo(BaseModel):
    total: int
    matched: int
    key_matches: List[str] = []


JobNotificationContent.model_rebuild()


class NotificationMessageBuilder:
    @staticmethod
    def build_apply_section(apply_url: Optional[str], job_post: JobPost) -> str:
        """Build apply link section."""
        if apply_url:
            return f"🔗 [Apply Here]({apply_url})"
        
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
    def build_notification_content(
        job_post: JobPost,
        overall_score: float,
        fit_score: float,
        want_score: Optional[float] = None,
        required_coverage: float = 0,
        apply_url: Optional[str] = None
    ) -> JobNotificationContent:
        """Build notification content from individual parameters."""
        
        job_info = JobInfo(
            title=getattr(job_post, 'title', None) or "Unknown Position",
            company=getattr(job_post, 'company', None) or "Unknown Company",
            location=NotificationMessageBuilder.format_location(job_post),
            is_remote=getattr(job_post, 'is_remote', False),
            salary=NotificationMessageBuilder.format_salary(job_post),
            job_type=getattr(job_post, 'job_type', None),
            job_level=getattr(job_post, 'job_level', None),
        )
        
        match_info = MatchInfo(
            overall_score=float(overall_score),
            fit_score=float(fit_score),
            want_score=float(want_score) if want_score else None,
            required_coverage=float(required_coverage),
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
        )
        
        match_info = MatchInfo(
            overall_score=match_data.get('overall_score', 0),
            fit_score=match_data.get('fit_score', 0),
            want_score=match_data.get('want_score'),
            required_coverage=match_data.get('required_coverage', 0),
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
        job_info = JobInfo(
            title=getattr(job_post, 'title', None) or "Unknown Position",
            company=getattr(job_post, 'company', None) or "Unknown Company",
            location=NotificationMessageBuilder.format_location(job_post),
            is_remote=getattr(job_post, 'is_remote', False) or False,
            salary=NotificationMessageBuilder.format_salary(job_post),
            job_type=getattr(job_post, 'job_type', None),
            job_level=getattr(job_post, 'job_level', None),
        )
        
        match_info = MatchInfo(
            overall_score=float(getattr(job_match, 'overall_score', 0) or 0),
            fit_score=float(getattr(job_match, 'fit_score', 0) or 0),
            want_score=float(getattr(job_match, 'want_score', None)) if getattr(job_match, 'want_score', None) else None,
            required_coverage=float(getattr(job_match, 'required_coverage', 0) or 0),
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
        
        lines.append(f"🎯 **{content.job.title}**")
        lines.append(f"🏢 {content.job.company}")
        lines.append(content.job.location)
        
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
        score_lines = [f"📊 **{content.match.overall_score:.0f}%** Match"]
        score_lines.append(f"   Fit: {content.match.fit_score:.0f}% | Coverage: {coverage:.0f}%")
        if content.match.want_score:
            score_lines.append(f"   Want: {content.match.want_score:.0f}%")
        lines.append("\n".join(score_lines))
        
        lines.append("")
        req_matched = content.requirements.matched
        req_total = content.requirements.total
        if req_total > 0:
            lines.append(f"✅ **{req_matched}/{req_total}** requirements matched")
        else:
            lines.append(f"⚠️ **{req_matched}/{req_total}** requirements matched")
        
        if content.apply_url:
            lines.append("")
            lines.append(f"🔗 [Apply Here]({content.apply_url})")
        
        lines.append("")
        lines.append(f"🔍 View details: /api/matches")
        
        return "\n".join(lines)
    
    @staticmethod
    def to_html(content: JobNotificationContent) -> str:
        """Convert notification content to HTML format (for Telegram/Email)."""
        markdown = NotificationMessageBuilder.to_markdown(content)
        
        html = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', markdown)
        html = html.replace("🎯 ", "<b>🎯 ")
        html = html.replace("🏢 ", "</b><br/>🏢 ")
        html = html.replace("💰 ", "<br/>💰 ")
        html = html.replace("📊 ", "<br/>📊 ")
        html = html.replace("✅ ", "<br/>✅ ")
        html = html.replace("🔗 ", "<br/>🔗 ")
        html = html.replace("🔍 ", "<br/>🔍 ")
        html = html.replace("─" * 40, "<hr/>")
        
        return html
    
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
            {"name": "📊 Match Score", "value": f"**{content.match.overall_score:.0f}%**", "inline": True},
            {"name": "🎯 Fit Score", "value": f"{content.match.fit_score:.0f}%", "inline": True},
            {"name": "📈 Coverage", "value": f"{coverage:.0f}%", "inline": True},
        ])
        
        req_matched = content.requirements.matched
        req_total = content.requirements.total
        if req_total > 0:
            fields.append({
                "name": "✅ Requirements",
                "value": f"{req_matched}/{req_total} matched",
                "inline": False
            })
        
        if content.apply_url:
            fields.append({
                "name": "🔗 Apply",
                "value": f"[Apply Here]({content.apply_url})",
                "inline": True
            })
        
        color = NotificationMessageBuilder._get_score_color(content.match.overall_score)
        
        return {
            "title": f"🎯 {content.job.title}",
            "description": f"Match Score: **{content.match.overall_score:.0f}%**",
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
