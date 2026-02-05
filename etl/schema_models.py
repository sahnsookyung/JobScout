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
    tech_keywords: List[str] = Field(description="Technologies, tools, and frameworks mentioned")
    
    def to_embedding_text(self) -> str:
        """Generate text representation for embedding."""
        parts = []
        if self.company:
            parts.append(self.company)
        if self.title:
            parts.append(self.title)
        if self.description:
            parts.append(self.description)
        return " - ".join(parts) if parts else ""


class EducationItem(BaseModel):
    """A single education entry."""
    model_config = ConfigDict(extra='forbid')
    
    degree: Optional[str] = Field(description="Degree earned (e.g., 'Bachelor of Science')")
    field_of_study: Optional[str] = Field(description="Field or major")
    description: Optional[str] = Field(description="Additional details about education")


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


class Projects(BaseModel):
    """Projects section of a resume."""
    model_config = ConfigDict(extra='forbid')
    
    description: Optional[str] = Field(description="Description of key projects")


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


# Generate OpenAI-compatible schema
EXTRACTION_SCHEMA = {
    "name": "job_extraction_schema",
    "strict": True,
    "schema": JobExtraction.model_json_schema()
}


# ============================================================================
# FACET EXTRACTION SCHEMA MODELS
# ============================================================================

class FacetExtraction(BaseModel):
    """Per-facet text extraction for Want score matching."""
    model_config = ConfigDict(extra='forbid')
    
    remote_flexibility: str = Field(
        description="Text about remote work, WFH policies, location independence"
    )
    compensation: str = Field(
        description="Text about salary, bonuses, equity, benefits"
    )
    learning_growth: str = Field(
        description="Text about learning opportunities, mentorship, career development"
    )
    company_culture: str = Field(
        description="Text about company values, DEI, work environment"
    )
    work_life_balance: str = Field(
        description="Text about working hours, PTO, burnout prevention"
    )
    tech_stack: str = Field(
        description="Text about technologies, tools, frameworks used"
    )
    visa_sponsorship: str = Field(
        description="Text about visa sponsorship and relocation assistance"
    )


# Generate OpenAI-compatible schema
FACET_EXTRACTION_SCHEMA_FOR_WANTS = {
    "name": "facet_extraction_schema",
    "strict": True,
    "schema": FacetExtraction.model_json_schema()
}
