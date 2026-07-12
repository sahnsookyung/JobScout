import AxeBuilder from '@axe-core/playwright';
import { expect, test, type Page, type Route } from '@playwright/test';

const MATCH_ID = '00000000-0000-0000-0000-000000000101';
const JOB_ID = '00000000-0000-0000-0000-000000000201';

const policy = {
    min_fit: 55,
    top_k: 50,
    min_jd_required_coverage: null,
    llm_judge_enabled: true,
    llm_judge_top_n: 5,
};

const stats = {
    success: true,
    stats: {
        total_matches: 1,
        active_matches: 1,
        hidden_count: 0,
        below_threshold_count: 0,
        qualifying_count: 1,
        min_fit_threshold: 55,
        score_distribution: { excellent: 1, good: 0, average: 0, poor: 0 },
        total_scored: 1,
        primary_count: 1,
        excluded_count: 0,
        excluded_by_reason: {},
        job_post_total: 1,
        active_job_posts: 1,
    },
};

const match = {
    match_id: MATCH_ID,
    job_id: JOB_ID,
    title: 'Senior Backend Engineer',
    company: 'Example Labs',
    location: 'Tokyo, Japan',
    is_remote: true,
    fit_score: 88,
    preference_score: 82,
    penalties: 0,
    required_coverage: 0.9,
    preferred_requirement_coverage: 0.8,
    match_type: 'strong_match',
    is_hidden: false,
    created_at: '2026-07-12T00:00:00Z',
    calculated_at: '2026-07-12T00:00:00Z',
    ranking_mode_used: 'balanced',
    dominant_reason_code: 'required_coverage',
    explanation_label: 'Strong evidence coverage',
    balanced_primary_score: 0.88,
    missing_scores: [],
    selection_tier: 'primary',
};

async function fulfillJson(route: Route, body: unknown, status = 200) {
    await route.fulfill({
        status,
        contentType: 'application/json',
        body: JSON.stringify(body),
    });
}

async function mockApi(page: Page) {
    await page.route('**/api/**', async (route) => {
        const request = route.request();
        const url = new URL(request.url());
        const path = url.pathname;

        if (path === `/api/matches/${MATCH_ID}`) {
            return fulfillJson(route, {
                success: true,
                match: {
                    ...match,
                    resume_fingerprint: 'resume-1',
                    fit_components: {},
                    preference_components: {},
                    fit_confidence: 0.9,
                    fit_explanation: {},
                    fit_scorer: {},
                    base_score: 88,
                    total_requirements: 1,
                    matched_requirements_count: 1,
                    status: 'active',
                    penalty_details: {},
                },
                job: {
                    job_id: JOB_ID,
                    title: match.title,
                    company: match.company,
                    location: match.location,
                    is_remote: true,
                    description: 'Build reliable APIs and data pipelines.',
                    salary_min: null,
                    salary_max: null,
                    currency: null,
                    min_years_experience: 5,
                    requires_degree: false,
                    security_clearance: false,
                    job_level: 'senior',
                },
                requirements: [],
            });
        }
        if (path === `/api/matches/${MATCH_ID}/explanation`) {
            return fulfillJson(route, { success: true, match_id: MATCH_ID, explanation: {} });
        }
        if (path === `/api/matches/${MATCH_ID}/llm-evaluations`) {
            return fulfillJson(route, { success: true, count: 0, evaluations: [] });
        }
        if (path === '/api/resume-variants') {
            return fulfillJson(route, { success: true, count: 0, variants: [] });
        }
        if (path === '/api/matches') {
            return fulfillJson(route, {
                success: true,
                count: 1,
                total: 1,
                has_more: false,
                matches: [match],
            });
        }
        if (path === '/api/matches/summary' || path === '/api/stats') {
            return fulfillJson(route, stats);
        }
        if (path === '/api/v1/policy') return fulfillJson(route, policy);
        if (path === '/api/config/scoring-weights') {
            return fulfillJson(route, { fit_score_source: 'deterministic' });
        }
        if (path === '/api/pipeline/active') return fulfillJson(route, null);
        if (path === '/api/pipeline/resume-eligibility') {
            return fulfillJson(route, {
                can_run: true,
                status: 'ready',
                message: 'Resume is ready.',
                retryable: false,
            });
        }
        if (path === '/api/pipeline/sources') {
            return fulfillJson(route, {
                success: true,
                api_based_fetching: true,
                total_count: 0,
                filtered_count: 0,
                seed_websites: [],
                sources: [],
            });
        }
        if (path === '/api/jobs') {
            return fulfillJson(route, {
                success: true,
                count: 0,
                total: 0,
                limit: 25,
                offset: 0,
                jobs: [],
            });
        }
        if (path === '/api/jobs/processing-blockers') {
            return fulfillJson(route, { success: true, count: 0, blockers: [] });
        }
        if (path === '/api/pipeline-runs') {
            return fulfillJson(route, {
                success: true,
                count: 0,
                total: 0,
                limit: 10,
                offset: 0,
                runs: [],
            });
        }
        if (path === '/api/pipeline-runs/llm-evaluations/queue') {
            return fulfillJson(route, {
                success: true,
                ready: true,
                queue: 'llm-evaluations',
                queued: 0,
                started: 0,
                deferred: 0,
                scheduled: 0,
                failed: 0,
            });
        }
        if (
            path === '/api/cloud/integrations'
            || path === '/api/cloud/integrations/sources'
            || path === '/api/cloud/integrations/sources/history'
        ) {
            return fulfillJson(route, []);
        }
        if (path === '/api/v1/candidate-preferences') {
            return fulfillJson(route, {
                enabled: false,
                natural_language_wants: '',
                version: 1,
            });
        }
        if (path === '/api/v1/notification-settings') {
            return fulfillJson(route, {
                enabled: false,
                minimum_fit_score: 80,
                channels: [],
            });
        }

        return fulfillJson(route, {});
    });
}

async function expectNoSeriousAccessibilityViolations(page: Page) {
    // Color contrast is tracked as existing theme debt; keep this lean gate focused on
    // structural and interaction regressions until the palette is redesigned deliberately.
    const results = await new AxeBuilder({ page }).disableRules(['color-contrast']).analyze();
    const serious = results.violations.filter(
        (violation) => violation.impact === 'serious' || violation.impact === 'critical',
    );
    expect(serious).toEqual([]);
}

test.beforeEach(async ({ page }) => {
    await mockApi(page);
    await page.goto('/');
});

test('reviews matches and opens job details', async ({ page }) => {
    await expect(page.getByRole('tab', { name: 'Jobs' })).toHaveAttribute('aria-selected', 'true');
    await expect(page.getByRole('button', { name: /View details for Senior Backend Engineer/ })).toBeVisible();
    await page.getByRole('button', { name: /View details for Senior Backend Engineer/ }).click();
    await expect(page.getByText('Build reliable APIs and data pipelines.')).toBeVisible();
    await expectNoSeriousAccessibilityViolations(page);
});

test('switches to the job management workspace', async ({ page }) => {
    await page.getByRole('tab', { name: 'Job Management' }).click();
    await expect(page.getByRole('tabpanel', { name: 'Job Management' })).toBeVisible();
    await expect(page.getByText(/source/i).first()).toBeVisible();
    await expectNoSeriousAccessibilityViolations(page);
});
