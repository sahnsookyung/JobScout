import React from 'react';
import { MapPin, Building2, Laptop } from 'lucide-react';
import { ModalShell } from '@/components/ui/ModalShell';
import { useMatchDetails } from '@/hooks/useMatchDetails';
import { Badge } from '@/components/ui/Badge';
import { formatScore, formatSalary } from '@/utils/formatters';

type MatchDetailsModalProps = Readonly<{
    matchId: string | null;
    onClose: () => void;
}>;

type SemanticVerdict = Readonly<{
    requirement_id: string;
    verdict: 'covered' | 'partial' | 'missing';
    reason?: string;
    semantic_score?: number;
    evidence_text?: string | null;
    evidence_section?: string | null;
}>;

type RetrievalExplanation = Readonly<{
    mode?: 'dense' | 'hybrid';
    sources?: string[];
}>;

type FitDiagnosticsExplanation = Readonly<{
    effective_fit_mode?: string;
    provider_route?: string;
    fallback_used?: boolean;
    fallback_reason?: string;
}>;

function evidenceToneLabel(score: number | null | undefined): string {
    if (typeof score !== 'number') return 'unscored';
    if (score >= 0.7) return 'strong';
    if (score >= 0.4) return 'moderate';
    return 'weak';
}

function formatDiagnosticLabel(value: string | null | undefined): string | null {
    if (typeof value !== 'string') {
        return null;
    }
    return value.replace(/_/g, ' ');
}

function preferenceStatusMessage(preferenceStatus: any): string | null {
    if (!preferenceStatus) {
        return null;
    }

    if (preferenceStatus.applied) {
        return 'Preferences applied.';
    }

    return `Preferences skipped: ${preferenceStatus.reason ?? 'unconfigured'}.`;
}


function LoadingState() {
    return (
        <div className="flex justify-center py-16">
            <div className="flex items-center gap-3 text-[13px] text-ink-soft">
                <span className="relative flex h-2 w-2">
                    <span className="ember absolute inset-0 rounded-full bg-accent opacity-40" aria-hidden="true" />
                    <span className="relative m-auto h-1 w-1 rounded-full bg-accent" />
                </span>
                <span>Loading match</span>
            </div>
        </div>
    );
}

function ErrorState({ message }: Readonly<{ message: string }>) {
    return (
        <div className="py-12 text-center">
            <p className="border border-warn/40 bg-warn-soft px-4 py-3 text-[13px] text-ink">{message}</p>
        </div>
    );
}

function ScoreDisplay({ label, value, emphasis }: Readonly<{ label: string; value: number; emphasis?: boolean }>) {
    const tone = emphasis && value >= 80 ? 'text-accent' : 'text-ink';
    return (
        <div className="border border-rule bg-surface p-6">
            <p className="caption">{label}</p>
            <div className={`display-numeral mt-2 text-[56px] tabular-nums ${tone}`}>
                {formatScore(value)}
            </div>
            <div className="mt-4 h-px bg-rule">
                <div
                    className={`h-[2px] -translate-y-px ${emphasis && value >= 80 ? 'bg-accent' : 'bg-ink-soft'} transition-[width] duration-700 ease-out`}
                    style={{ width: `${value}%` }}
                />
            </div>
        </div>
    );
}

function JobInfoSection({ job }: Readonly<{ job: any }>) {
    const hasSalary = Boolean(job.salary_min || job.salary_max);

    return (
        <section>
            <h3 className="text-[24px] font-medium leading-tight tracking-tight text-ink">{job.title}</h3>

            <div className="mt-4 flex flex-wrap items-center gap-x-6 gap-y-2 text-[13px] text-ink-soft">
                <span className="inline-flex items-center gap-1.5">
                    <Building2 className="h-3.5 w-3.5 text-ink-muted" aria-hidden="true" />
                    <span className="text-ink">{job.company}</span>
                </span>
                {job.location && (
                    <span className="inline-flex items-center gap-1.5">
                        <MapPin className="h-3.5 w-3.5 text-ink-muted" aria-hidden="true" />
                        <span>{job.location}</span>
                    </span>
                )}
                {job.is_remote && (
                    <span className="inline-flex items-center gap-1.5 text-accent">
                        <Laptop className="h-3.5 w-3.5" aria-hidden="true" />
                        <span>Remote</span>
                    </span>
                )}
            </div>

            <dl className="mt-6 grid grid-cols-2 gap-px overflow-hidden border border-rule bg-rule md:grid-cols-4">
                {hasSalary && (
                    <div className="bg-surface px-4 py-3">
                        <dt className="caption">Salary</dt>
                        <dd className="mt-1 text-[14px] text-ink tabular-nums">
                            {formatSalary(job.salary_min, job.salary_max, job.currency)}
                        </dd>
                    </div>
                )}
                {(job.min_years_experience !== null && job.min_years_experience !== undefined) && (
                    <div className="bg-surface px-4 py-3">
                        <dt className="caption">Experience</dt>
                        <dd className="mt-1 text-[14px] text-ink tabular-nums">{job.min_years_experience}+ years</dd>
                    </div>
                )}
                {job.job_level && (
                    <div className="bg-surface px-4 py-3">
                        <dt className="caption">Level</dt>
                        <dd className="mt-1 text-[14px] text-ink">{job.job_level}</dd>
                    </div>
                )}
                {(job.requires_degree !== null && job.requires_degree !== undefined) && (
                    <div className="bg-surface px-4 py-3">
                        <dt className="caption">Degree</dt>
                        <dd className="mt-1 text-[14px] text-ink">{job.requires_degree ? 'Required' : 'Not required'}</dd>
                    </div>
                )}
            </dl>
        </section>
    );
}

function ScoresSection({ match }: Readonly<{ match: any }>) {
    const fitExplanation = match.fit_explanation;
    const semanticSummary = typeof fitExplanation?.summary === 'string' ? fitExplanation.summary : null;
    const fitConfidence = typeof match.fit_confidence === 'number' ? match.fit_confidence : null;
    const scorerName = typeof match.fit_scorer?.name === 'string' ? match.fit_scorer.name : null;
    const retrieval = fitExplanation?.retrieval as RetrievalExplanation | undefined;
    const diagnostics = fitExplanation?.diagnostics as FitDiagnosticsExplanation | undefined;
    const preferenceSummary = preferenceStatusMessage(match.preference_status);

    let retrievalMode: string | null = null;
    if (retrieval?.mode === 'hybrid') retrievalMode = 'Hybrid retrieval';
    else if (retrieval?.mode === 'dense') retrievalMode = 'Dense retrieval';

    const retrievalSources = Array.isArray(retrieval?.sources) ? retrieval.sources.join(' + ') : null;
    const fitMode = formatDiagnosticLabel(diagnostics?.effective_fit_mode);
    const providerRoute = formatDiagnosticLabel(diagnostics?.provider_route);

    let fallbackMessage: string | null = null;
    if (typeof fitExplanation?.message === 'string') {
        fallbackMessage = fitExplanation.message;
    } else if (diagnostics?.fallback_used) {
        fallbackMessage = 'Semantic fit fallback was used for this match.';
    }

    return (
        <section>
            <p className="caption">Scores</p>
            <h4 className="mt-1 text-[18px] font-medium text-ink">How this one adds up</h4>

            <div className="mt-5 grid grid-cols-1 gap-4 md:grid-cols-2">
                <ScoreDisplay label="Fit" value={match.fit_score ?? 0} emphasis />
                <ScoreDisplay label="Preference" value={(match.preference_score ?? 0) * 100} />
            </div>

            <dl className="mt-4 grid grid-cols-2 gap-px overflow-hidden border border-rule bg-rule md:grid-cols-4">
                <div className="bg-surface px-4 py-3">
                    <dt className="caption">Required coverage</dt>
                    <dd className="display-numeral mt-1 text-[22px] text-ink tabular-nums">
                        {formatScore(match.required_coverage * 100)}
                    </dd>
                </div>
                <div className="bg-surface px-4 py-3">
                    <dt className="caption">Preferred coverage</dt>
                    <dd className="display-numeral mt-1 text-[22px] text-ink tabular-nums">
                        {formatScore(match.preferred_requirement_coverage * 100)}
                    </dd>
                </div>
                <div className="bg-surface px-4 py-3">
                    <dt className="caption">Matched requirements</dt>
                    <dd className="display-numeral mt-1 text-[22px] text-ink tabular-nums">
                        {match.matched_requirements_count} / {match.total_requirements}
                    </dd>
                </div>
                <div className="bg-surface px-4 py-3">
                    <dt className="caption">Penalties</dt>
                    <dd className="display-numeral mt-1 text-[22px] text-ink tabular-nums">
                        {match.penalties.toFixed(1)}
                    </dd>
                </div>
            </dl>

            {semanticSummary && (
                <div className="mt-5 border border-rule bg-surface p-6">
                    <div className="flex flex-wrap items-center gap-2">
                        <Badge variant="accent">Semantic fit</Badge>
                        {fitConfidence !== null && (
                            <span className="caption">
                                Confidence <span className="tabular-nums text-ink">{formatScore(fitConfidence * 100)}</span>
                            </span>
                        )}
                        {scorerName && (
                            <span className="caption">{scorerName.replaceAll('_', ' ')}</span>
                        )}
                        {retrievalMode && <span className="caption">{retrievalMode}</span>}
                        {fitMode && <span className="caption">{fitMode}</span>}
                        {providerRoute && <span className="caption">{providerRoute}</span>}
                    </div>
                    <p className="mt-3 text-[14px] leading-relaxed text-ink-soft">{semanticSummary}</p>
                    {retrievalSources && (
                        <p className="mt-3 text-[13px] text-ink-muted">
                            Candidate generation used {retrievalSources}.
                        </p>
                    )}
                    {fallbackMessage && (
                        <p className="mt-3 border-l-2 border-warn/60 pl-3 text-[13px] text-ink-soft">
                            {fallbackMessage}
                        </p>
                    )}
                    {preferenceSummary && (
                        <p className="mt-3 text-[13px] text-ink-muted">
                            {preferenceSummary}
                        </p>
                    )}
                </div>
            )}
        </section>
    );
}

function RequirementCard({
    req,
    verdict,
}: Readonly<{
    req: any;
    verdict?: SemanticVerdict;
}>) {
    const isRequired = req.req_type === 'required';
    const verdictLabel = verdict?.verdict ?? (req.is_covered ? 'covered' : 'missing');
    const isCovered = verdictLabel === 'covered';
    const isPartial = verdictLabel === 'partial';
    const evidenceText = verdict?.evidence_text ?? req.evidence_text;
    const evidenceSection = verdict?.evidence_section ?? req.evidence_section;
    const reason = typeof verdict?.reason === 'string' ? verdict.reason : null;
    const evidenceScore = typeof req.evidence_score === 'number'
        ? req.evidence_score
        : (typeof verdict?.semantic_score === 'number' ? verdict.semantic_score : null);
    const vectorScore = typeof req.similarity_score === 'number' ? req.similarity_score : null;
    const toneLabel = evidenceToneLabel(evidenceScore);

    let verdictBadgeVariant: 'success' | 'warning' | 'error' = 'error';
    let verdictBadgeLabel = 'Missing';
    if (isCovered) { verdictBadgeVariant = 'success'; verdictBadgeLabel = 'Covered'; }
    else if (isPartial) { verdictBadgeVariant = 'warning'; verdictBadgeLabel = 'Partial'; }

    let borderTone = 'border-rule';
    if (isCovered) {
        borderTone = 'border-affirm/40';
    } else if (isPartial) {
        borderTone = 'border-warn/40';
    }

    return (
        <div className={`border ${borderTone} bg-surface p-5`}>
            <div className="mb-3 flex flex-wrap items-start justify-between gap-3">
                <div className="flex items-center gap-2">
                    <Badge variant={isRequired ? 'info' : 'default'}>
                        {isRequired ? 'Required' : 'Preferred'}
                    </Badge>
                    <Badge variant={verdictBadgeVariant}>{verdictBadgeLabel}</Badge>
                </div>
                {typeof evidenceScore === 'number' && (
                    <span
                        className="caption tabular-nums"
                        title={vectorScore !== null ? `Vector similarity ${vectorScore.toFixed(2)}` : undefined}
                    >
                        Evidence <span className="text-ink">{evidenceScore.toFixed(2)}</span> · {toneLabel}
                    </span>
                )}
            </div>

            <p className="text-[14px] text-ink">{req.requirement_text || 'No description'}</p>

            {reason && (
                <div className="mt-3 border-l-2 border-rule pl-3">
                    <p className="caption">Why</p>
                    <p className="mt-1 text-[13px] text-ink-soft">{reason}</p>
                </div>
            )}

            {evidenceText && (
                <div className="mt-3 border-l-2 border-accent/60 pl-3">
                    <p className="caption">Evidence</p>
                    <p className="mt-1 text-[13px] text-ink-soft">{evidenceText}</p>
                    {evidenceSection && (
                        <p className="mt-1 text-[12px] text-ink-muted">Source: {evidenceSection}</p>
                    )}
                </div>
            )}
        </div>
    );
}

function RequirementsSection({
    requirements,
    fitExplanation,
}: Readonly<{
    requirements: any[];
    fitExplanation?: { requirement_verdicts?: SemanticVerdict[] } | null;
}>) {
    const requiredReqs = requirements.filter((req) => req.req_type === 'required');
    const preferredReqs = requirements.filter((req) => req.req_type === 'preferred');
    const verdicts = Array.isArray(fitExplanation?.requirement_verdicts)
        ? fitExplanation.requirement_verdicts
        : [];
    const verdictById = new Map(verdicts.map((verdict) => [verdict.requirement_id, verdict]));

    const requiredCovered = requiredReqs.filter((req) => req.is_covered).length;
    const preferredCovered = preferredReqs.filter((req) => req.is_covered).length;

    return (
        <section>
            <p className="caption">Requirements</p>
            <h4 className="mt-1 text-[18px] font-medium text-ink">What the job asks for</h4>

            <RequirementGroup
                title="Required"
                requirements={requiredReqs}
                coveredCount={requiredCovered}
                verdictById={verdictById}
                className="mt-5"
            />

            <RequirementGroup
                title="Preferred"
                requirements={preferredReqs}
                coveredCount={preferredCovered}
                verdictById={verdictById}
                className="mt-8"
            />
        </section>
    );
}

function RequirementGroup({
    title,
    requirements,
    coveredCount,
    verdictById,
    className,
}: Readonly<{
    title: string;
    requirements: any[];
    coveredCount: number;
    verdictById: Map<string, SemanticVerdict>;
    className: string;
}>) {
    if (requirements.length === 0) {
        return null;
    }

    return (
        <div className={className}>
            <div className="flex items-baseline justify-between border-b border-rule pb-2">
                <h5 className="text-[14px] font-medium text-ink">
                    {title} <span className="text-ink-muted tabular-nums">({requirements.length})</span>
                </h5>
                <span className="caption tabular-nums">
                    <span className="text-ink">{coveredCount}</span>/{requirements.length} covered
                </span>
            </div>
            <div className="mt-3 space-y-2">
                {requirements.map((req) => (
                    <RequirementCard
                        key={req.requirement_id}
                        req={req}
                        verdict={verdictById.get(req.requirement_id)}
                    />
                ))}
            </div>
        </div>
    );
}

function JobDescriptionSection({ description }: Readonly<{ description: string }>) {
    return (
        <section>
            <p className="caption">Description</p>
            <h4 className="mt-1 text-[18px] font-medium text-ink">The original posting</h4>
            <div className="mt-4 border-l-2 border-rule pl-5">
                <p className="whitespace-pre-wrap text-[14px] leading-relaxed text-ink-soft">{description}</p>
            </div>
        </section>
    );
}

function ModalBody({ isLoading, data }: Readonly<{ isLoading: boolean; data: any }>) {
    if (isLoading) return <LoadingState />;
    if (!data) return <ErrorState message="Failed to load match details" />;

    return (
        <div className="space-y-10">
            <JobInfoSection job={data.job} />
            <ScoresSection match={data.match} />
            <RequirementsSection requirements={data.requirements} fitExplanation={data.match.fit_explanation} />
            {data.job.description && <JobDescriptionSection description={data.job.description} />}
        </div>
    );
}

export const MatchDetailsModal: React.FC<MatchDetailsModalProps> = ({ matchId, onClose }) => {
    const isOpen = Boolean(matchId);

    const { data, isLoading } = useMatchDetails(matchId);

    if (!isOpen) return null;

    return (
        <ModalShell
            isOpen={isOpen}
            onClose={onClose}
            titleId="match-details-title"
            eyebrow="Review"
            title="Match details"
            closeLabel="Close match details"
            maxWidth="max-w-5xl"
        >
            <ModalBody isLoading={isLoading} data={data} />
        </ModalShell>
    );
};
