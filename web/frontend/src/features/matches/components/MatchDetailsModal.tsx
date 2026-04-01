// MatchDetailsModal.tsx
import React, { useEffect } from 'react';
import { X, MapPin, Building2, Laptop, XCircle, Award, Sparkles } from 'lucide-react';
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

function useEscapeKey(onClose: () => void, enabled: boolean) {
    useEffect(() => {
        if (!enabled) return;
        const handleEscape = (e: KeyboardEvent) => {
            if (e.key === 'Escape') onClose();
        };
        globalThis.addEventListener('keydown', handleEscape);
        return () => globalThis.removeEventListener('keydown', handleEscape);
    }, [enabled, onClose]);
}

function ModalShell({
    title,
    onClose,
    children,
}: Readonly<{
    title: string;
    onClose: () => void;
    children: React.ReactNode;
}>) {
    return (
        <div className="fixed inset-0 z-50 overflow-y-auto">
            <div
                className="fixed inset-0 bg-black/60 backdrop-blur-sm transition-opacity"
                onClick={onClose}
                aria-hidden="true"
            />

            <div className="flex min-h-full items-center justify-center p-4">
                <div className="relative w-full max-w-5xl bg-white rounded-3xl shadow-2xl overflow-hidden">
                    {/* Header with gradient */}
                    <div className="relative bg-gradient-to-r from-blue-600 via-indigo-600 to-purple-600 p-8">
                        <div className="absolute inset-0 bg-gradient-to-b from-transparent to-black/10" />
                        <div className="relative flex items-center justify-between">
                            <h2 className="text-3xl font-black text-white">{title}</h2>
                            <button
                                onClick={onClose}
                                className="p-2 text-white/80 hover:text-white hover:bg-white/20 rounded-xl transition-all duration-200"
                                aria-label="Close"
                            >
                                <X className="w-6 h-6" />
                            </button>
                        </div>
                    </div>

                    <div className="p-8 max-h-[75vh] overflow-y-auto">
                        {children}
                    </div>
                </div>
            </div>
        </div>
    );
}

function LoadingState() {
    return (
        <div className="flex justify-center py-16">
            <div className="relative">
                <div className="absolute inset-0 bg-blue-400 blur-xl opacity-50 animate-pulse" />
                <div className="relative animate-spin rounded-full h-16 w-16 border-4 border-gray-200 border-t-blue-600" />
            </div>
        </div>
    );
}

function ErrorState({ message }: Readonly<{ message: string }>) {
    return (
        <div className="text-center py-16">
            <div className="inline-flex items-center gap-3 px-6 py-4 bg-red-50 text-red-700 rounded-2xl border-2 border-red-200">
                <XCircle className="w-6 h-6" />
                <span className="font-semibold">{message}</span>
            </div>
        </div>
    );
}

function ScoreDisplay({ label, value, gradient }: Readonly<{ label: string; value: number; gradient: string }>) {
    return (
        <div className="relative">
            <div className={`absolute inset-0 bg-gradient-to-br ${gradient} opacity-10 blur-xl rounded-2xl`} />
            <div className="relative bg-white rounded-2xl p-6 border-2 border-gray-100 shadow-lg">
                <div className="text-sm font-bold text-gray-600 uppercase tracking-wider mb-2">{label}</div>
                <div className={`text-5xl font-black bg-gradient-to-br ${gradient} bg-clip-text text-transparent`}>
                    {formatScore(value)}
                </div>
                {/* Progress bar */}
                <div className="mt-4 h-2 bg-gray-200 rounded-full overflow-hidden">
                    <div
                        className={`h-full bg-gradient-to-r ${gradient} transition-all duration-1000 ease-out`}
                        style={{ width: `${value}%` }}
                    />
                </div>
            </div>
        </div>
    );
}

function JobInfoSection({ job }: Readonly<{ job: any }>) {
    const hasSalary = Boolean(job.salary_min || job.salary_max);

    return (
        <section className="bg-gradient-to-br from-slate-50 to-blue-50 rounded-2xl p-8 border-2 border-blue-100">
            <div className="flex items-start justify-between mb-6">
                <div className="flex-1">
                    <h3 className="text-3xl font-black text-gray-900 mb-4 leading-tight">{job.title}</h3>

                    <div className="flex flex-wrap items-center gap-4 mb-4">
                        <div className="flex items-center gap-2 px-4 py-2 bg-white rounded-xl shadow-sm border border-gray-200">
                            <Building2 className="w-5 h-5 text-gray-500" aria-hidden="true" />
                            <span className="font-bold text-gray-900">{job.company}</span>
                        </div>

                        {job.location && (
                            <div className="flex items-center gap-2 px-4 py-2 bg-white rounded-xl shadow-sm border border-gray-200">
                                <MapPin className="w-5 h-5 text-gray-500" aria-hidden="true" />
                                <span className="font-medium text-gray-700">{job.location}</span>
                            </div>
                        )}

                        {job.is_remote && (
                            <div className="flex items-center gap-2 px-4 py-2 bg-gradient-to-r from-blue-500 to-indigo-500 text-white rounded-xl shadow-md font-bold">
                                <Laptop className="w-4 h-4" aria-hidden="true" />
                                <span>Remote</span>
                            </div>
                        )}
                    </div>
                </div>
            </div>

            {/* Job details grid */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                {hasSalary && (
                    <div className="bg-white p-4 rounded-xl border border-gray-200">
                        <div className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-1">Salary</div>
                        <div className="font-black text-gray-900">{formatSalary(job.salary_min, job.salary_max, job.currency)}</div>
                    </div>
                )}

                {(job.min_years_experience !== null && job.min_years_experience !== undefined) && (
                    <div className="bg-white p-4 rounded-xl border border-gray-200">
                        <div className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-1">Experience</div>
                        <div className="font-black text-gray-900">{job.min_years_experience}+ years</div>
                    </div>
                )}

                {job.job_level && (
                    <div className="bg-white p-4 rounded-xl border border-gray-200">
                        <div className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-1">Level</div>
                        <div className="font-black text-gray-900">{job.job_level}</div>
                    </div>
                )}

                {(job.requires_degree !== null && job.requires_degree !== undefined) && (
                    <div className="bg-white p-4 rounded-xl border border-gray-200">
                        <div className="text-xs font-bold text-gray-500 uppercase tracking-wider mb-1">Degree</div>
                        <div className="font-black text-gray-900">{job.requires_degree ? 'Required' : 'Not Required'}</div>
                    </div>
                )}
            </div>
        </section>
    );
}

function ScoresSection({ match }: Readonly<{ match: any }>) {
    const isHighScore = match.overall_score >= 80;
    const fitExplanation = match.fit_explanation;
    const semanticSummary = typeof fitExplanation?.summary === 'string' ? fitExplanation.summary : null;
    const fitConfidence = typeof match.fit_confidence === 'number' ? match.fit_confidence : null;
    const scorerName = typeof match.fit_scorer?.name === 'string' ? match.fit_scorer.name : null;

    return (
        <section>
            {isHighScore && (
                <div className="mb-6 inline-flex items-center gap-2 px-4 py-2 bg-gradient-to-r from-yellow-400 to-orange-400 text-white rounded-xl shadow-lg font-black">
                    <Award className="w-5 h-5" aria-hidden="true" />
                    <span>Exceptional Match!</span>
                    <Sparkles className="w-4 h-4" aria-hidden="true" />
                </div>
            )}

            <h4 className="text-xl font-black text-gray-900 mb-6">Match Scores</h4>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
                <ScoreDisplay label="Overall" value={match.overall_score} gradient="from-blue-500 to-indigo-600" />
                <ScoreDisplay label="Fit" value={match.fit_score ?? 0} gradient="from-blue-400 to-blue-600" />
            </div>

            {/* Coverage details */}
            <div className="grid grid-cols-2 gap-4 bg-gray-50 p-6 rounded-2xl border border-gray-200">
                <div>
                    <div className="text-xs font-bold text-gray-600 uppercase tracking-wider mb-1">Required Coverage</div>
                    <div className="text-2xl font-black text-gray-900">{formatScore(match.required_coverage * 100)}</div>
                </div>
                <div>
                    <div className="text-xs font-bold text-gray-600 uppercase tracking-wider mb-1">Preferred Coverage</div>
                    <div className="text-2xl font-black text-gray-900">{formatScore(match.preferred_coverage * 100)}</div>
                </div>
                <div>
                    <div className="text-xs font-bold text-gray-600 uppercase tracking-wider mb-1">Matched Requirements</div>
                    <div className="text-2xl font-black text-gray-900">
                        {match.matched_requirements_count} / {match.total_requirements}
                    </div>
                </div>
                <div>
                    <div className="text-xs font-bold text-gray-600 uppercase tracking-wider mb-1">Penalties</div>
                    <div className="text-2xl font-black text-gray-900">{match.penalties.toFixed(1)}</div>
                </div>
            </div>

            {semanticSummary && (
                <div className="mt-6 rounded-2xl border-2 border-blue-100 bg-gradient-to-br from-blue-50 to-indigo-50 p-6">
                    <div className="flex flex-wrap items-center gap-3 mb-3">
                        <Badge variant="info" className="font-bold">Semantic Fit</Badge>
                        {fitConfidence !== null && (
                            <span className="text-sm font-bold text-blue-700">
                                Confidence {formatScore(fitConfidence * 100)}
                            </span>
                        )}
                        {scorerName && (
                            <span className="text-xs font-bold uppercase tracking-wider text-gray-500">
                                {scorerName.replaceAll('_', ' ')}
                            </span>
                        )}
                    </div>
                    <p className="text-base font-medium text-gray-800 leading-relaxed">{semanticSummary}</p>
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
    let cardToneClasses = 'bg-gray-50 border-gray-200 hover:border-gray-300';
    let verdictBadgeVariant: 'success' | 'warning' | 'error' = 'error';
    let verdictBadgeLabel = '✗ Missing';

    if (isCovered) {
        cardToneClasses = 'bg-gradient-to-br from-blue-50 to-indigo-50 border-blue-200 hover:border-blue-300';
        verdictBadgeVariant = 'success';
        verdictBadgeLabel = '✓ Covered';
    } else if (isPartial) {
        cardToneClasses = 'bg-gradient-to-br from-amber-50 to-orange-50 border-amber-200 hover:border-amber-300';
        verdictBadgeVariant = 'warning';
        verdictBadgeLabel = '△ Partial';
    }

    return (
        <div className={`p-5 rounded-2xl border-2 transition-all duration-200 ${cardToneClasses}`}>
            <div className="flex items-start justify-between gap-4 mb-3">
                <div className="flex items-center gap-2">
                    <Badge variant={isRequired ? 'info' : 'default'} className="font-bold">
                        {isRequired ? 'Required' : 'Preferred'}
                    </Badge>
                    <Badge variant={verdictBadgeVariant} className="font-bold">
                        {verdictBadgeLabel}
                    </Badge>
                </div>
                {verdict && (
                    <div className="text-xs font-bold text-gray-500 bg-white px-3 py-1 rounded-lg">
                        Semantic review
                    </div>
                )}
            </div>

            <div className="font-semibold text-gray-900 mb-3">
                {req.requirement_text || 'No description'}
            </div>

            {reason && (
                <div className="mb-3 rounded-lg border border-blue-100 bg-white/80 p-3">
                    <div className="text-xs font-bold text-blue-600 uppercase tracking-wider mb-1">Why this verdict</div>
                    <div className="text-sm text-gray-700">{reason}</div>
                </div>
            )}

            {evidenceText && (
                <div className="p-3 bg-white rounded-lg border border-gray-200">
                    <div className="text-xs font-bold text-blue-600 uppercase tracking-wider mb-1">Evidence Found</div>
                    <div className="text-sm text-gray-700">{evidenceText}</div>
                    {evidenceSection && (
                        <div className="text-xs text-gray-500 mt-1">Source: {evidenceSection}</div>
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
            <h4 className="text-xl font-black text-gray-900 mb-6">Requirements Analysis</h4>

            {requiredReqs.length > 0 && (
                <div className="mb-8">
                    <div className="flex items-center justify-between mb-4">
                        <h5 className="text-lg font-bold text-gray-900">Required ({requiredReqs.length})</h5>
                        <div className="flex items-center gap-2">
                            <div className="text-sm font-bold text-gray-600">{requiredCovered}/{requiredReqs.length} covered</div>
                            <div className="w-32 h-2 bg-gray-200 rounded-full overflow-hidden">
                                <div
                                    className="h-full bg-gradient-to-r from-blue-500 to-blue-600 transition-all duration-500"
                                    style={{ width: `${(requiredCovered / requiredReqs.length) * 100}%` }}
                                />
                            </div>
                        </div>
                    </div>
                    <div className="space-y-3">
                        {requiredReqs.map((req) => (
                            <RequirementCard
                                key={req.requirement_id}
                                req={req}
                                verdict={verdictById.get(req.requirement_id)}
                            />
                        ))}
                    </div>
                </div>
            )}

            {preferredReqs.length > 0 && (
                <div>
                    <div className="flex items-center justify-between mb-4">
                        <h5 className="text-lg font-bold text-gray-900">Preferred ({preferredReqs.length})</h5>
                        <div className="flex items-center gap-2">
                            <div className="text-sm font-bold text-gray-600">{preferredCovered}/{preferredReqs.length} covered</div>
                            <div className="w-32 h-2 bg-gray-200 rounded-full overflow-hidden">
                                <div
                                    className="h-full bg-gradient-to-r from-indigo-500 to-purple-500 transition-all duration-500"
                                    style={{ width: `${(preferredCovered / preferredReqs.length) * 100}%` }}
                                />
                            </div>
                        </div>
                    </div>
                    <div className="space-y-3">
                        {preferredReqs.map((req) => (
                            <RequirementCard
                                key={req.requirement_id}
                                req={req}
                                verdict={verdictById.get(req.requirement_id)}
                            />
                        ))}
                    </div>
                </div>
            )}
        </section>
    );
}

function JobDescriptionSection({ description }: Readonly<{ description: string }>) {
    return (
        <section>
            <h4 className="text-xl font-black text-gray-900 mb-4">Job Description</h4>
            <div className="prose prose-sm max-w-none bg-gray-50 p-6 rounded-2xl border border-gray-200">
                <p className="text-gray-700 whitespace-pre-wrap leading-relaxed">{description}</p>
            </div>
        </section>
    );
}

function ModalBody({ isLoading, data }: Readonly<{ isLoading: boolean; data: any }>) {
    if (isLoading) return <LoadingState />;
    if (!data) return <ErrorState message="Failed to load match details" />;

    return (
        <div className="space-y-8">
            <JobInfoSection job={data.job} />
            <ScoresSection match={data.match} />
            <RequirementsSection requirements={data.requirements} fitExplanation={data.match.fit_explanation} />
            {data.job.description && <JobDescriptionSection description={data.job.description} />}
        </div>
    );
}

export const MatchDetailsModal: React.FC<MatchDetailsModalProps> = ({ matchId, onClose }) => {
    const isOpen = Boolean(matchId);
    useEscapeKey(onClose, isOpen);

    const { data, isLoading } = useMatchDetails(matchId);

    if (!isOpen) return null;

    return (
        <ModalShell title="Match Details" onClose={onClose}>
            <ModalBody isLoading={isLoading} data={data} />
        </ModalShell>
    );
};
