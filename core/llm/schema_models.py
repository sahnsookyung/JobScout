"""
Pydantic models for JSON schemas used in AI extractions.

This module provides:
1. Type-safe Python models for all extraction schemas
2. Runtime JSON schema generation for OpenAI structured output
3. Helper methods for embedding text generation

All schemas follow OpenAI's structured output requirements with strict validation.
"""
from typing import List, Optional, Literal
from pydantic import BaseModel, Field, ConfigDict


# ============================================================================
# RESUME SCHEMA MODELS
# ============================================================================

class PartialDate(BaseModel):
    """A date with variable precision (year, month, or unknown)."""
    model_config = ConfigDict(extra='forbid')
    
    text: Optional[str] = Field(description="Original date text as extracted")
    year: Optional[int] = Field(description="Year component")
    month: Optional[int] = Field(description="Month component (1-12)")
    precision: Literal["unknown", "year", "month"] = Field(
        description="How precise the date is"
    )


class ExperienceItem(BaseModel):
    """A single work experience entry."""
    model_config = ConfigDict(extra='forbid')

    company: Optional[str] = Field(description="Company or organization name")
    title: Optional[str] = Field(description="Job title or role")
    start_date: Optional[PartialDate] = Field(description="Start date of employment")
    end_date: Optional[PartialDate] = Field(description="End date of employment (null if current)")
    is_current: Optional[bool] = Field(description="Whether this is the current position")
    description: Optional[str] = Field(description="Job description or responsibilities")
    years_value: Optional[float] = Field(description="Years of experience gained at this position (extracted from description)")
    tech_keywords: List[str] = Field(description="Technologies, tools, and frameworks mentioned")

    highlights: List[str] = Field(default=[], description="Bullet points, key achievements, or highlights")

    def to_embedding_text(self) -> str:
        """Generate text representation for embedding."""
        parts = []
        if self.company:
            parts.append(self.company)
        if self.title:
            parts.append(self.title)
        if self.description:
            parts.append(self.description)
        if self.highlights:
            parts.extend(self.highlights)
        return " - ".join(parts) if parts else ""


class EducationItem(BaseModel):
    """A single education entry."""
    model_config = ConfigDict(extra='forbid')
    
    degree: Optional[str] = Field(description="Degree earned (e.g., 'Bachelor of Science')")
    field_of_study: Optional[str] = Field(description="Field or major")
    institution: Optional[str] = Field(description="School, university, or institution name")
    graduation_year: Optional[int] = Field(description="Year of graduation or completion")
    description: Optional[str] = Field(description="Additional details about education")
    highlights: List[str] = Field(default=[], description="Key achievements, honors, or highlights")


class SkillItem(BaseModel):
    """A single skill with proficiency and experience."""
    model_config = ConfigDict(extra='forbid')
    
    name: Optional[str] = Field(description="Skill name (e.g., 'Python', 'Project Management')")
    kind: Optional[str] = Field(description="Category of skill (e.g., 'language', 'framework', 'soft_skill')")
    proficiency: Optional[str] = Field(description="Proficiency level (e.g., 'beginner', 'intermediate', 'expert')")
    years_experience: Optional[float] = Field(description="Years of experience with this skill")
    
    def to_embedding_text(self) -> str:
        """Generate text representation for embedding."""
        parts = []
        if self.name:
            parts.append(self.name)
        if self.proficiency:
            parts.append(f"{self.proficiency} level")
        if self.years_experience:
            parts.append(f"{self.years_experience} years")
        return " - ".join(parts) if parts else ""


class SkillGroup(BaseModel):
    """A group of related skills."""
    model_config = ConfigDict(extra='forbid')
    
    group_name: Optional[str] = Field(description="Name of the skill group (e.g., 'Languages', 'Frameworks')")
    items: List[SkillItem] = Field(description="Skills in this group")


class SkillsBlock(BaseModel):
    """Complete skills section of a resume."""
    model_config = ConfigDict(extra='forbid')
    
    groups: List[SkillGroup] = Field(description="Skills organized by category")
    all: List[SkillItem] = Field(description="All skills as a flat list")
    
    def to_embedding_text(self) -> str:
        """Generate text representation for embedding."""
        skill_names = [skill.name for skill in self.all if skill.name]
        return ", ".join(skill_names) if skill_names else ""


class CertificationItem(BaseModel):
    """A professional certification."""
    model_config = ConfigDict(extra='forbid')
    
    name: Optional[str] = Field(description="Certification name")
    issuer: Optional[str] = Field(description="Organization that issued the certification")
    issued_year: Optional[int] = Field(description="Year certification was earned")
    expires_year: Optional[int] = Field(description="Year certification expires (null if no expiration)")


class LanguageItem(BaseModel):
    """A language proficiency entry."""
    model_config = ConfigDict(extra='forbid')
    
    language: Optional[str] = Field(description="Language name")
    proficiency: Optional[str] = Field(description="Proficiency level (e.g., 'native', 'fluent', 'conversational')")


class ProjectItem(BaseModel):
    """A single project entry."""
    model_config = ConfigDict(extra='forbid')
    
    name: Optional[str] = Field(description="Project name or title")
    description: Optional[str] = Field(description="Project description and accomplishments")
    technologies: List[str] = Field(description="Technologies and tools used")
    url: Optional[str] = Field(description="Project URL (GitHub, demo, etc.)")
    date: Optional[PartialDate] = Field(description="Project date or time period")
    highlights: List[str] = Field(default=[], description="Key achievements or highlights")


class Projects(BaseModel):
    """Notable projects from resume."""
    model_config = ConfigDict(extra='forbid')
    
    items: List[ProjectItem] = Field(description="List of individual projects")


class Summary(BaseModel):
    """Professional summary section."""
    model_config = ConfigDict(extra='forbid')
    
    text: Optional[str] = Field(description="Professional summary or objective statement")
    total_experience_years: Optional[float] = Field(
        description="Total years of professional experience as stated by the candidate"
    )


class Profile(BaseModel):
    """Complete profile extracted from resume."""
    model_config = ConfigDict(extra='forbid')
    
    summary: Summary = Field(description="Professional summary and claimed experience")
    experience: List[ExperienceItem] = Field(description="Work experience history")
    projects: Projects = Field(description="Notable projects")
    education: List[EducationItem] = Field(description="Educational background")
    skills: SkillsBlock = Field(description="Skills and competencies")
    certifications: List[CertificationItem] = Field(description="Professional certifications")
    languages: List[LanguageItem] = Field(description="Language proficiencies")


class Extraction(BaseModel):
    """Metadata about the extraction process."""
    model_config = ConfigDict(extra='forbid')
    
    # Note: For OpenAI strict mode, we can't have default values
    # but we use Optional[X] to allow null values
    confidence: Optional[float] = Field(
        description="Confidence score of extraction (0.0-1.0)",
        ge=0.0,
        le=1.0
    )
    warnings: List[str] = Field(description="Warnings or issues during extraction")


class ResumeSchema(BaseModel):
    """Complete resume extraction schema."""
    model_config = ConfigDict(extra='forbid')
    
    profile: Profile = Field(description="Structured resume content")
    extraction: Extraction = Field(description="Extraction metadata")
    
    @property
    def claimed_total_years(self) -> Optional[float]:
        """Get the claimed total years of experience."""
        return self.profile.summary.total_experience_years


# Generate OpenAI-compatible schema
RESUME_SCHEMA = {
    "name": "resume_schema_v1.0",
    "strict": True,
    "schema": ResumeSchema.model_json_schema()
}


# ============================================================================
# JOB EXTRACTION SCHEMA MODELS
# ============================================================================

class JobRequirement(BaseModel):
    """A single job requirement (must-have, nice-to-have, or responsibility)."""
    model_config = ConfigDict(extra='forbid')
    
    req_type: Literal["must_have", "nice_to_have", "responsibility"] = Field(
        description="Type of requirement"
    )
    category: Literal["technical", "soft_skill", "domain_knowledge", "logistical"] = Field(
        description="Category of requirement for matching purposes"
    )
    text: str = Field(description="The requirement text, cleaned and normalized")
    related_skills: List[str] = Field(
        description="Specific skills mentioned in this requirement"
    )
    proficiency: Optional[str] = Field(
        description="Required proficiency level (basic, proficient, expert, unspecified)"
    )


class JobBenefit(BaseModel):
    """A single job benefit or perk."""
    model_config = ConfigDict(extra='forbid')
    
    category: Literal[
        "health_insurance", "pension", "pto", "remote_work",
        "parental_leave", "learning_budget", "equipment",
        "wellness", "other"
    ] = Field(description="Type of benefit")
    text: str = Field(description="Description of the benefit")


JOB_OFFERINGS_PROFILE_VERSION = 1


class JobOfferingSignal(BaseModel):
    """A single evidence-backed working-condition or perk signal."""
    model_config = ConfigDict(extra='forbid')

    label: str = Field(description="Short normalized offering label")
    evidence: str = Field(description="Short source snippet from the job description")
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence in this signal")


class JobOfferingsProfile(BaseModel):
    """Cached job-side profile used for candidate preference matching."""
    model_config = ConfigDict(extra='forbid')

    schema_version: int = Field(description="Job offerings profile schema version")
    work_arrangement: Optional[str] = Field(
        description="Remote/hybrid/onsite/flexible arrangement when stated",
    )
    location_timezone: List[JobOfferingSignal] = Field(
        description="Location, timezone, relocation, or travel signals"
    )
    visa_sponsorship: Optional[bool] = Field(
        description="Whether the role explicitly offers visa sponsorship",
    )
    compensation: List[JobOfferingSignal] = Field(
        description="Salary, equity, bonus, or compensation transparency signals"
    )
    benefits_perks: List[JobOfferingSignal] = Field(
        description="Benefits and perks explicitly offered by the role"
    )
    flexibility: List[JobOfferingSignal] = Field(
        description="Schedule, remote, hybrid, async, PTO, or flexibility signals"
    )
    team_culture: List[JobOfferingSignal] = Field(
        description="Team culture, collaboration style, and working environment signals"
    )
    mentorship_growth: List[JobOfferingSignal] = Field(
        description="Mentorship, learning, promotion, or career growth signals"
    )
    product_domain: List[JobOfferingSignal] = Field(
        description="Product, mission, customer, or domain signals"
    )
    tech_environment: List[JobOfferingSignal] = Field(
        description="Tools, languages, architecture, or engineering-practice signals"
    )
    negative_signals: List[JobOfferingSignal] = Field(
        description="Explicitly stated working-condition signals that may conflict with preferences"
    )
    evidence_snippets: List[str] = Field(
        description="Short source snippets supporting the profile"
    )
    confidence: float = Field(ge=0.0, le=1.0, description="Overall confidence in the offerings profile")


class JobExtraction(BaseModel):
    """Complete job posting extraction."""
    model_config = ConfigDict(extra='forbid')
    
    thought_process: str = Field(
        description="Brief analysis of tech stack, seniority level, and key constraints"
    )
    job_summary: str = Field(description="One-sentence summary of the role")
    seniority_level: Optional[str] = Field(
        description="Inferred seniority (Intern, Junior, Mid-Level, Senior, Staff/Principal, Lead/Manager, Unspecified)"
    )
    remote_policy: Optional[str] = Field(
        description="Remote work policy (On-site, Hybrid, Remote (Local), Remote (Global), Unspecified)"
    )
    visa_sponsorship_available: Optional[bool] = Field(
        description="Whether visa sponsorship is offered"
    )
    min_years_experience: Optional[int] = Field(
        description="Minimum years of experience required"
    )
    requires_degree: Optional[bool] = Field(
        description="Whether a degree is a hard requirement"
    )
    security_clearance: Optional[bool] = Field(
        description="Whether security clearance is required"
    )
    salary_min: Optional[float] = Field(description="Minimum salary if mentioned")
    salary_max: Optional[float] = Field(description="Maximum salary if mentioned")
    currency: Optional[str] = Field(
        description="Currency code (USD, JPY, EUR, etc.)"
    )
    tech_stack: List[str] = Field(
        description="List of technologies, languages, and frameworks mentioned"
    )
    requirements: List[JobRequirement] = Field(
        description="Detailed requirements and responsibilities"
    )
    benefits: List[JobBenefit] = Field(
        description="Job benefits and perks"
    )
    offerings_profile: Optional[JobOfferingsProfile] = Field(
        description="Cached job-side working conditions, perks, and culture profile",
    )


# Generate OpenAI-compatible schema
EXTRACTION_SCHEMA = {
    "name": "job_extraction_schema",
    "strict": True,
    "schema": JobExtraction.model_json_schema()
}


# Convenience aliases for clarity (optional, for gradual migration)
EXTRACTION_SCHEMA_SPEC = EXTRACTION_SCHEMA
EXTRACTION_JSON_SCHEMA = EXTRACTION_SCHEMA["schema"]

RESUME_SCHEMA_SPEC = RESUME_SCHEMA
RESUME_JSON_SCHEMA = RESUME_SCHEMA["schema"]
