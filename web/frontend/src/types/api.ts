// Generated from Pydantic models

export type RankingMode = 'preference_first' | 'fit_first' | 'balanced';

export interface MatchSummary {
    match_id: string;
    job_id: string | null;
    title: string;
    company: string;
    location: string | null;
    is_remote: boolean | null;
    fit_score: number | null;
    preference_score: number | null;
    penalties: number;
    required_coverage: number;
    preferred_requirement_coverage: number;
    match_type: string;
    is_hidden: boolean;
    created_at: string | null;
    calculated_at: string | null;
    // Ranking explanation fields
    ranking_mode_used: string | null;
    dominant_reason_code: string | null;
    explanation_label: string | null;
    balanced_primary_score: number | null;
    missing_scores: string[];
    scoring_degraded_reason?: string | null;
    selection_tier?: 'primary' | 'excluded';
    excluded_reason?: string | null;
}

export interface RequirementDetail {
    requirement_id: string;
    requirement_text: string | null;
    evidence_text: string | null;
    evidence_section: string | null;
    similarity_score: number;
    evidence_score?: number | null;
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
    preference_score: number | null;
    fit_components: Record<string, any> | null;
    preference_components: Record<string, any> | null;
    fit_confidence: number | null;
    fit_explanation: Record<string, any> | null;
    fit_scorer: Record<string, any> | null;
    scoring_degraded_reason?: string | null;
    preference_status?: Record<string, any> | null;
    base_score: number;
    penalties: number;
    required_coverage: number;
    preferred_requirement_coverage: number;
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

export interface MatchExplanationResponse {
    success: boolean;
    match_id: string;
    explanation: Record<string, any> | null;
    message?: string | null;
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
        total_scored: number;
        primary_count: number;
        excluded_count: number;
        excluded_by_reason: Record<string, number>;
        preference_status?: Record<string, any> | null;
    };
}

export interface ScoringWeights {
    fit_score_source: string;
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
    effective_recipient?: string | null;
    override_address?: string | null;
    override_status?: 'none' | 'pending' | 'verified' | 'expired' | null;
    override_verified_at?: string | null;
}

export interface NotificationSettings {
    notifications_enabled: boolean;
    min_fit_for_alerts: number;
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
    min_fit_for_alerts: number;
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

export interface NotificationEmailOverrideRequest {
    address: string;
}

export interface NotificationEmailVerificationRequest {
    token: string;
}

export interface NotificationEmailOverrideResponse {
    success: boolean;
    message: string;
    channel?: NotificationChannelSettings | null;
}

export interface CandidatePreferences {
    remote_mode: 'any' | 'remote' | 'hybrid' | 'onsite';
    target_locations: string[];
    visa_sponsorship_required: boolean;
    salary_min: number | null;
    employment_types: string[];
    soft_preferences: string;
    soft_preference_summary?: string | null;
    preference_mode: 'semantic_rerank' | 'llm_judge';
    allowed_preference_modes: Array<'semantic_rerank' | 'llm_judge'>;
    effective_preference_mode: 'semantic_rerank' | 'llm_judge';
    revision: number;
}

export interface CandidatePreferencesUpdateRequest {
    remote_mode: 'any' | 'remote' | 'hybrid' | 'onsite';
    target_locations: string[];
    visa_sponsorship_required: boolean;
    salary_min: number | null;
    employment_types: string[];
    soft_preferences: string;
    preference_mode: 'semantic_rerank' | 'llm_judge';
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

export interface CloudUser {
    id: string;
    email: string;
    name: string;
    picture?: string | null;
    provider: string;
    token_kind: string;
    session_expires_at?: number | null;
}

export interface CloudTenant {
    id: string;
    name: string;
    role: 'owner' | 'admin' | 'member';
    is_default: boolean;
}

export interface CloudAuthExchangeResponse {
    access_token?: string | null;
    token_type: string;
    user: CloudUser;
    tenants?: CloudTenant[];
    selected_tenant_id?: string | null;
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

export interface FetchSource {
    site_type: string;
    display_name: string;
    seed_url?: string | null;
    description?: string | null;
    tags: string[];
    search_keywords: string[];
    fetch_mode: string;
    search_term?: string | null;
    location?: string | null;
    country?: string | null;
    results_wanted: number;
    hours_old?: number | null;
    options: Record<string, any>;
    api_health?: {
        available: boolean;
        status: string;
        endpoint?: string | null;
        status_code?: number | null;
        response_time_ms?: number | null;
        error?: string | null;
    } | null;
}

export interface FetchSourcesResponse {
    success: boolean;
    jobspy_url?: string | null;
    api_based_fetching: boolean;
    search_query?: string | null;
    total_count: number;
    filtered_count: number;
    seed_websites: string[];
    sources: FetchSource[];
}

export type MatchStatus = 'active' | 'stale' | 'all';
export type PolicyPreset = 'strict' | 'balanced' | 'discovery';
