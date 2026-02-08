// Generated from Pydantic models
export interface MatchSummary {
    match_id: string;
    job_id: string | null;
    title: string;
    company: string;
    location: string | null;
    is_remote: boolean | null;
    fit_score: number | null;
    want_score: number | null;
    overall_score: number;
    base_score: number;
    penalties: number;
    required_coverage: number;
    preferred_coverage: number;
    match_type: string;
    is_hidden: boolean;
    created_at: string | null;
    calculated_at: string | null;
}

export interface RequirementDetail {
    requirement_id: string;
    requirement_text: string | null;
    evidence_text: string | null;
    evidence_section: string | null;
    similarity_score: number;
    is_covered: boolean;
    req_type: string;
}

export interface JobDetails {
    job_id: string | null;
    title: string | null;
    company: string | null;
    location: string | null;
    is_remote: boolean | null;
    description: string | null;
    salary_min: number | null;
    salary_max: number | null;
    currency: string | null;
    min_years_experience: number | null;
    requires_degree: boolean | null;
    security_clearance: boolean | null;
    job_level: string | null;
}

export interface MatchDetail {
    match_id: string;
    resume_fingerprint: string;
    fit_score: number | null;
    want_score: number | null;
    overall_score: number;
    fit_components: Record<string, any> | null;
    want_components: Record<string, any> | null;
    fit_weight: number | null;
    want_weight: number | null;
    base_score: number;
    penalties: number;
    required_coverage: number;
    preferred_coverage: number;
    total_requirements: number;
    matched_requirements_count: number;
    match_type: string;
    status: string;
    created_at: string | null;
    calculated_at: string | null;
    penalty_details: Record<string, any>;
}

export interface MatchDetailResponse {
    success: boolean;
    match: MatchDetail;
    job: JobDetails;
    requirements: RequirementDetail[];
}

export interface MatchesResponse {
    success: boolean;
    count: number;
    matches: MatchSummary[];
}

export interface StatsResponse {
    success: boolean;
    stats: {
        total_matches: number;
        active_matches: number;
        hidden_count: number;
        below_threshold_count: number;
        min_fit_threshold: number;
        score_distribution: {
            excellent: number;
            good: number;
            average: number;
            poor: number;
        };
    };
}

export interface ScoringWeights {
    fit_weight: number;
    want_weight: number;
    facet_weights: Record<string, number>;
}

export interface PolicyConfig {
    min_fit: number;
    top_k: number;
    min_jd_required_coverage: number | null;
}

export interface PipelineTaskResponse {
    success: boolean;
    task_id: string;
    message: string;
}

export interface PipelineStatusResponse {
    task_id: string;
    status: 'pending' | 'running' | 'completed' | 'failed';
    matches_count?: number;
    saved_count?: number;
    notified_count?: number;
    error?: string;
    execution_time?: number;
    step?: string;
}

export type MatchStatus = 'active' | 'stale' | 'all';
export type SortBy = 'overall' | 'fit' | 'want';
export type PolicyPreset = 'strict' | 'balanced' | 'discovery';
