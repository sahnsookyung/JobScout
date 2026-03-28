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

export interface ResumeEligibilityResponse {
    can_run: boolean;
    status: string;
    message: string;
    retryable: boolean;
    upload_id?: string;
    resume_hash?: string;
    task_id?: string;
}

export interface ResumePreflightResponse {
    status: string;
    message: string;
    retryable: boolean;
    can_skip_upload: boolean;
    resume_hash: string;
    upload_id?: string;
    task_id?: string;
}

export interface ResumeUploadResponse {
    success: boolean;
    resume_hash: string;
    message: string;
    upload_id?: string;
    task_id?: string;
    status?: string;
}

export interface ResumeStatusResponse {
    task_id: string;
    status: string;
    step?: string;
    message?: string;
    error?: string;
}

export interface NotificationChannelSettings {
    enabled: boolean;
    configured: boolean;
    available: boolean;
    availability_reason?: string | null;
    masked_recipient?: string | null;
    last_test_status?: string | null;
    last_tested_at?: string | null;
    last_test_error?: string | null;
}

export interface NotificationSettings {
    notifications_enabled: boolean;
    min_score_threshold: number;
    notify_on_new_match: boolean;
    notify_on_batch_complete: boolean;
    revision: number;
    channels: Record<string, NotificationChannelSettings>;
}

export interface NotificationChannelSettingsUpdate {
    enabled: boolean;
    secret_value?: string | null;
}

export interface NotificationSettingsUpdateRequest {
    notifications_enabled: boolean;
    min_score_threshold: number;
    notify_on_new_match: boolean;
    notify_on_batch_complete: boolean;
    channels: Record<string, NotificationChannelSettingsUpdate>;
}

export interface NotificationSettingsTestRequest {
    channel_type: string;
}

export interface NotificationSettingsTestResponse {
    success: boolean;
    notification_id?: string | null;
    message: string;
}

export interface ApiFieldError {
    path: string[];
    code: string;
    message: string;
}

export interface ApiErrorResponse {
    code: string;
    message: string;
    detail?: string;
    fields?: ApiFieldError[];
}

export interface PipelineStatusResponse {
    task_id: string;
    status:
        | 'pending'
        | 'running'
        | 'cancellation_requested'
        | 'persisting'
        | 'completed'
        | 'failed'
        | 'cancelled';
    upload_id?: string;
    resume_fingerprint?: string;
    matches_count?: number;
    saved_count?: number;
    notified_count?: number;
    error?: string;
    execution_time?: number;
    step?: string;
    stale_due_to_newer_upload?: boolean;
    latest_upload_id?: string;
    latest_resume_fingerprint?: string;
    stale_message?: string;
}

export type MatchStatus = 'active' | 'stale' | 'all';
export type SortBy = 'overall' | 'fit' | 'want';
export type PolicyPreset = 'strict' | 'balanced' | 'discovery';
