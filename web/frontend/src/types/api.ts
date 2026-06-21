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
    llm_evaluation_status?: string | null;
    llm_evaluation_id?: string | null;
    llm_score?: number | null;
    llm_confidence?: number | null;
    llm_judged_at?: string | null;
    llm_effective_for_rerank?: boolean;
    llm_ignored_for_rerank_reason?: string | null;
    llm_stale_status?: string | null;
    llm_original_rank?: number | null;
    llm_reranked_rank?: number | null;
    llm_rerank_score?: number | null;
    llm_rerank_confidence?: number | null;
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
    description_source?: string;
    description_completeness?: string;
    description_warning_code?: string | null;
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
    llm_evaluation_status?: string | null;
    llm_evaluation_id?: string | null;
    llm_score?: number | null;
    llm_confidence?: number | null;
    llm_judged_at?: string | null;
    llm_effective_for_rerank?: boolean;
    llm_ignored_for_rerank_reason?: string | null;
    llm_stale_status?: string | null;
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

export interface MatchLlmEvaluation {
    id: string;
    match_id?: string | null;
    job_id: string;
    status: string;
    llm_score?: number | null;
    confidence?: number | null;
    verdict?: string | null;
    summary?: string | null;
    reason_codes: string[];
    requirement_verdicts: Array<Record<string, any>>;
    analysis?: {
        transferable_strengths?: string[];
        gaps?: string[];
        ranking_rationale?: string;
        input_truncation?: Record<string, any>;
        [key: string]: any;
    };
    effective_for_rerank?: boolean;
    ignored_for_rerank_reason?: string | null;
    stale_status?: string | null;
    input_truncation?: Record<string, any>;
    provider: string;
    model: string;
    prompt_version: string;
    schema_version: string;
    error_code?: string | null;
    retryable: boolean;
    created_at?: string | null;
    started_at?: string | null;
    completed_at?: string | null;
}

export interface MatchLlmEvaluationListResponse {
    success: boolean;
    count: number;
    evaluations: MatchLlmEvaluation[];
}

export interface MatchLlmEvaluationMutationResponse {
    success: boolean;
    evaluation?: MatchLlmEvaluation | null;
    reused: boolean;
    accepted?: boolean;
    message: string;
}

export interface MatchesResponse {
    success: boolean;
    count: number;
    total?: number;
    limit?: number | null;
    offset?: number;
    has_more?: boolean;
    matches: MatchSummary[];
    llm_rerank?: {
        enabled: boolean;
        available: boolean;
        applied: boolean;
        top_n: number;
        window_size: number;
        eligible_count: number;
        reranked_count: number;
        reason?: string | null;
    };
    degraded?: boolean;
    degraded_reasons?: Array<{ code: string; detail: string }>;
}

export interface JobInventoryItem {
    job_id: string;
    title: string;
    company: string;
    location: string | null;
    is_remote: boolean | null;
    status: string;
    is_extracted: boolean;
    is_embedded: boolean;
    extraction_status: string;
    embedding_status: string;
    description_completeness: string;
    description_source: string;
    description_warning_code?: string | null;
    source_site?: string | null;
    source_url?: string | null;
    first_seen_at?: string | null;
    last_seen_at?: string | null;
    extraction_attempts: number;
    extraction_last_error?: string | null;
    extraction_next_retry_at?: string | null;
    embedding_attempts: number;
    embedding_last_error?: string | null;
    embedding_next_retry_at?: string | null;
}

export type JobProcessingStatus =
    | 'all'
    | 'ready'
    | 'extracted'
    | 'embedded'
    | 'pending_extraction'
    | 'pending_embedding'
    | 'failed';

export type JobLifecycleStatus = 'all' | 'active' | 'inactive' | 'expired' | 'unknown';

export interface JobsResponse {
    success: boolean;
    count: number;
    total: number;
    limit: number;
    offset: number;
    jobs: JobInventoryItem[];
}

export interface ProcessingBlockerItem {
    job_id: string;
    stage: 'extraction' | 'embedding' | 'matching' | string;
    blocker_code: string;
    blocker_detail: string;
    status: string;
    attempts: number;
    last_error?: string | null;
    retry_eligible: boolean;
    first_seen_at?: string | null;
    last_seen_at?: string | null;
    last_attempt_at?: string | null;
    next_retry_at?: string | null;
}

export interface ProcessingBlockersResponse {
    success: boolean;
    count: number;
    blockers: ProcessingBlockerItem[];
}

export interface PipelineRunStageSummary {
    id: string;
    stage: string;
    status: string;
    queued_count: number;
    processed_count: number;
    succeeded_count: number;
    failed_count: number;
    skipped_count: number;
    retry_count: number;
    retry_eligible: boolean;
    last_error?: string | null;
    started_at?: string | null;
    completed_at?: string | null;
    metadata: Record<string, any>;
}

export interface PipelineRunSummary {
    id: string;
    task_id: string;
    run_type: string;
    status: string;
    current_stage?: string | null;
    queued_count: number;
    processed_count: number;
    succeeded_count: number;
    failed_count: number;
    skipped_count: number;
    retry_eligible: boolean;
    last_error?: string | null;
    owner_id?: string | null;
    tenant_id?: string | null;
    resume_fingerprint?: string | null;
    started_at?: string | null;
    completed_at?: string | null;
    heartbeat_at?: string | null;
    created_at?: string | null;
    updated_at?: string | null;
    metadata: Record<string, any>;
    stages: PipelineRunStageSummary[];
    allowed_actions: string[];
}

export interface PipelineRunsResponse {
    success: boolean;
    count: number;
    total: number;
    limit: number;
    offset: number;
    runs: PipelineRunSummary[];
}

export interface PipelineRunDetailResponse {
    success: boolean;
    run: PipelineRunSummary;
}

export interface PipelineRunOperationResponse {
    success: boolean;
    action: string;
    message: string;
    run: PipelineRunSummary;
    source_run_id?: string | null;
    enqueued_task_id?: string | null;
}

export interface LlmEvaluationQueueStatusResponse {
    success: boolean;
    ready: boolean;
    queue: string;
    queued: number;
    started: number;
    deferred: number;
    scheduled: number;
    failed: number;
    error?: string | null;
}

export interface StatsResponse {
    success: boolean;
    stats: {
        total_matches: number;
        active_matches: number;
        hidden_count: number;
        below_threshold_count: number;
        beyond_top_k_count?: number;
        qualifying_count?: number;
        policy_top_k?: number | null;
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
        job_post_total?: number;
        active_job_posts?: number;
        inactive_job_posts?: number;
        extracted_job_posts?: number;
        embedded_job_posts?: number;
        ready_to_score_job_posts?: number;
        pending_extraction_job_posts?: number;
        processing_extraction_job_posts?: number;
        retryable_extraction_job_posts?: number;
        failed_extraction_job_posts?: number;
        pending_embedding_job_posts?: number;
        processing_embedding_job_posts?: number;
        retryable_embedding_job_posts?: number;
        failed_embedding_job_posts?: number;
    };
}

export interface ScoringWeights {
    fit_score_source: string;
}

export interface PolicyConfig {
    min_fit: number;
    top_k: number;
    min_jd_required_coverage: number | null;
    llm_judge_enabled?: boolean;
    llm_judge_top_n?: number;
    llm_judge_top_n_max?: number;
    llm_judge_available?: boolean;
    llm_judge_unavailable_reason?: string;
    llm_judge_revision?: number;
}

export type PolicyUpdatePayload = Pick<
    PolicyConfig,
    'min_fit' | 'top_k' | 'min_jd_required_coverage'
> & {
    llm_judge_enabled?: boolean;
    llm_judge_top_n?: number;
};

export interface PipelineTaskResponse {
    success: boolean;
    task_id: string;
    message: string;
}

export type PipelinePhase =
    | 'initializing'
    | 'loading_resume'
    | 'extracting_resume'
    | 'embedding_resume'
    | 'matching_jobs'
    | 'scoring'
    | 'saving'
    | 'notifying'
    | 'completed'
    | 'failed'
    | 'cancelled';

export interface ProcessingProgress {
    current_step: number;
    total_steps: number;
    percent: number;
    started_at?: string | null;
    updated_at?: string | null;
}

export interface ProcessingWarning {
    code: string;
    message: string;
}

export interface ProcessingFailure {
    code: string;
    user_message: string;
    retryable: boolean;
    next_action?: string | null;
}

export interface PipelineStats {
    jobs_imported?: number;
    jobs_processed?: number;
    jobs_extracted?: number;
    jobs_embedded?: number;
    jobs_seen?: number;
    jobs_ready_to_score?: number;
    jobs_pending_extraction?: number;
    jobs_pending_embedding?: number;
    candidates_considered?: number;
    matches_selected?: number;
    matches_saved?: number;
    below_threshold?: number;
    notifications_sent?: number;
    [key: string]: unknown;
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
    matching_task_id?: string | null;
    status?: string;
    phase?: PipelinePhase | string | null;
    progress?: ProcessingProgress | null;
    stats?: PipelineStats;
    warnings?: ProcessingWarning[];
    failure?: ProcessingFailure | null;
}

export interface ResumeStatusResponse {
    task_id: string;
    status: string;
    step?: string;
    matching_task_id?: string | null;
    phase?: PipelinePhase | string | null;
    progress?: ProcessingProgress | null;
    stats?: PipelineStats;
    warnings?: ProcessingWarning[];
    failure?: ProcessingFailure | null;
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
    phase?: PipelinePhase | string | null;
    progress?: ProcessingProgress | null;
    stats?: PipelineStats;
    warnings?: ProcessingWarning[];
    failure?: ProcessingFailure | null;
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
    provider_name?: string | null;
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
    external_fetch_status?: {
        enabled: boolean;
        configured: boolean;
        status: string;
        provider?: string | null;
        last_attempt_at?: string | null;
        last_success_at?: string | null;
        next_eligible_at?: string | null;
        failure_class?: string | null;
        budget_remaining?: number | null;
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

export interface SourceFetchResponse {
    success: boolean;
    source: string;
    status: string;
    fetched_count: number;
    imported_count: number;
    skipped_count: number;
    warnings: string[];
    next_eligible_at?: string | null;
    failure_class?: string | null;
    budget_remaining?: number | null;
}

export interface CloudIntegration {
    id: string;
    tenant_id: string;
    provider: string;
    display_name: string;
    status: string;
    sync_interval_minutes: number;
    config: Record<string, any>;
    capabilities: string[];
    validation_status: string;
    last_validated_at: string | null;
    last_error: string | null;
    source_kind?: 'user' | 'workspace';
    can_manage?: boolean;
    allowed_actions?: string[];
    status_reason?: string | null;
    deleted_at?: string | null;
    is_user_source?: boolean;
    owner_user_id?: string | null;
    source_url?: string | null;
    created_at?: string | null;
    updated_at?: string | null;
    initial_sync?: {
        status: string;
        provider: string;
        run_id?: string | null;
        jobs_seen: number;
        jobs_imported: number;
        jobs_deactivated: number;
        error_summary?: string | null;
        retry_after_seconds?: number | null;
    } | null;
}

export interface AtsSourceDiscoveryCandidate {
    provider: string;
    identifier: string;
    config_key: string;
    config: Record<string, any>;
    display_name: string;
    source_url: string | null;
    jobs_seen: number;
    match_reason: string;
}

export interface AtsSourceHistoryEvent {
    id: string;
    action: string;
    resource_id: string | null;
    provider: string | null;
    display_name: string | null;
    identifier: string | null;
    source_url: string | null;
    status: string | null;
    occurred_at: string | null;
    readd_payload: AtsSourceCreateRequest | null;
}

export interface UserAtsSource extends CloudIntegration {
    is_user_source: true;
    owner_user_id: string;
    source_url: string | null;
}

export interface AtsSourceCreateRequest {
    display_name?: string;
    source_url?: string;
    provider?: string;
    identifier?: string;
    providers?: string[];
    status?: string;
    sync_interval_minutes?: number;
}

export interface AtsSourceUpdateRequest {
    display_name?: string;
    source_url?: string;
    provider?: string;
    identifier?: string;
    providers?: string[];
    status?: string;
    sync_interval_minutes?: number;
}

export interface IntegrationUpdateRequest {
    display_name?: string;
    status?: string;
    sync_interval_minutes?: number;
    config?: Record<string, any>;
    secret_value?: string | null;
}

export interface SyncRunResponse {
    run_id: string;
    status: string;
    jobs_seen: number;
    jobs_imported: number;
    jobs_deactivated: number;
    provider: string;
    started_at?: string | null;
    completed_at?: string | null;
    duration_seconds?: number | null;
    error_summary?: string | null;
    is_manual?: boolean | null;
    dedupe_fingerprint_count: number;
}

export type ResumeVariantDownloadFormat = 'markdown' | 'html' | 'docx';

export interface ResumeVariantClaim {
    text: string;
    sources?: Array<Record<string, any>>;
}

export interface ResumeVariantExperience {
    title?: string | null;
    company?: string | null;
    bullets?: ResumeVariantClaim[];
    sources?: Array<Record<string, any>>;
}

export interface ResumeVariantContent {
    template_key?: string;
    tone?: string;
    job?: {
        title?: string | null;
        company?: string | null;
    };
    summary?: ResumeVariantClaim[];
    targeted_evidence?: ResumeVariantClaim[];
    skills?: ResumeVariantClaim[];
    experience?: ResumeVariantExperience[];
    gaps?: ResumeVariantClaim[];
    source_quality?: {
        job_description_completeness?: string | null;
        job_description_source?: string | null;
        job_description_warning_code?: string | null;
        fit_score?: number | null;
        required_coverage?: number | null;
    };
}

export interface ResumeVariant {
    id: string;
    match_id: string;
    job_post_id: string;
    template_key: string;
    generation_mode: string;
    created_at: string | null;
    content: ResumeVariantContent;
    evidence_map: Record<string, any>;
    warnings: string[];
    download_formats: ResumeVariantDownloadFormat[];
    reused?: boolean | null;
    quota_status?: {
        daily_remaining?: number;
        hourly_remaining?: number;
    } | null;
}

export interface ResumeVariantEnvelope {
    success: boolean;
    variant: ResumeVariant;
}

export interface ResumeVariantListResponse {
    success: boolean;
    count: number;
    variants: ResumeVariant[];
}

export type MatchStatus = 'active' | 'stale' | 'all';
export type PolicyPreset = 'strict' | 'balanced' | 'discovery';
