import React, { useEffect } from 'react';
import { X, MapPin, Building2 } from 'lucide-react';
import { useMatchDetails } from '@/hooks/useMatchDetails';
import { Badge } from '@/components/ui/Badge';
import { formatScore, formatSalary, getScoreBadgeColor } from '@/utils/formatters';

interface MatchDetailsModalProps {
    matchId: string | null;
    onClose: () => void;
}

function useEscapeKey(onClose: () => void, enabled: boolean) {
    useEffect(() => {
        if (!enabled) return;

        const handleEscape = (e: KeyboardEvent) => {
            if (e.key === 'Escape') onClose();
        };

        window.addEventListener('keydown', handleEscape);
        return () => window.removeEventListener('keydown', handleEscape);
    }, [enabled, onClose]);
}

function ModalShell({
    title,
    onClose,
    children,
}: {
    title: string;
    onClose: () => void;
    children: React.ReactNode;
}) {
    return (
        <div className="fixed inset-0 z-50 overflow-y-auto">
            <div
                className="fixed inset-0 bg-black bg-opacity-50 transition-opacity"
                onClick={onClose}
            />

            <div className="flex min-h-full items-center justify-center p-4">
                <div className="relative w-full max-w-4xl bg-white rounded-lg shadow-xl">
                    <div className="flex items-center justify-between p-6 border-b">
                        <h2 className="text-2xl font-bold text-gray-900">{title}</h2>
                        <button
                            onClick={onClose}
                            className="text-gray-400 hover:text-gray-600 transition-colors"
                            aria-label="Close"
                        >
                            <X className="w-6 h-6" />
                        </button>
                    </div>

                    <div className="p-6 max-h-[70vh] overflow-y-auto">{children}</div>
                </div>
            </div>
        </div>
    );
}

function LoadingState() {
    return (
        <div className="flex justify-center py-12">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600" />
        </div>
    );
}

function ErrorState({ message }: { message: string }) {
    return <div className="text-center text-gray-500 py-12">{message}</div>;
}

function IconText({
    icon,
    children,
}: {
    icon: React.ReactNode;
    children: React.ReactNode;
}) {
    return (
        <div className="flex items-center gap-1">
            {icon}
            <span>{children}</span>
        </div>
    );
}

function InfoItem({ label, value }: { label: string; value: React.ReactNode }) {
    return (
        <div>
            <div className="text-xs text-gray-500">{label}</div>
            <div className="font-medium">{value}</div>
        </div>
    );
}

function ScoreBadge({ label, score }: { label: string; score: number | null | undefined }) {
    if (score === null || score === undefined) return null;

    return (
        <Badge className={getScoreBadgeColor(score)}>
            {label}: {formatScore(score)}
        </Badge>
    );
}

function JobInfoSection({ job }: { job: any }) {
    const hasSalary = Boolean(job.salary_min || job.salary_max);
    const hasExperience = job.min_years_experience !== null && job.min_years_experience !== undefined;
    const hasDegree = job.requires_degree !== null && job.requires_degree !== undefined;

    return (
        <section>
            <h3 className="text-xl font-semibold mb-3">{job.title}</h3>

            <div className="flex flex-wrap gap-4 text-sm text-gray-600 mb-4">
                <IconText icon={<Building2 className="w-4 h-4" />}>{job.company}</IconText>

                {job.location ? (
                    <IconText icon={<MapPin className="w-4 h-4" />}>{job.location}</IconText>
                ) : null}

                {job.is_remote ? <Badge variant="info">Remote</Badge> : null}
            </div>

            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
                {hasSalary ? (
                    <InfoItem
                        label="Salary"
                        value={formatSalary(job.salary_min, job.salary_max, job.currency)}
                    />
                ) : null}

                {hasExperience ? (
                    <InfoItem label="Experience" value={`${job.min_years_experience}+ years`} />
                ) : null}

                {job.job_level ? <InfoItem label="Level" value={job.job_level} /> : null}

                {hasDegree ? (
                    <InfoItem
                        label="Degree"
                        value={job.requires_degree ? 'Required' : 'Not Required'}
                    />
                ) : null}
            </div>
        </section>
    );
}

function ScoresSection({ match }: { match: any }) {
    return (
        <section>
            <h4 className="font-semibold mb-3">Match Scores</h4>

            <div className="flex gap-3">
                <ScoreBadge label="Overall" score={match.overall_score} />
                <ScoreBadge label="Fit" score={match.fit_score ?? 0} />
                <ScoreBadge label="Want" score={match.want_score} />
            </div>

            <div className="mt-3 grid grid-cols-2 gap-3 text-sm">
                <div>
                    <span className="text-gray-600">Required Coverage:</span>{' '}
                    <span className="font-medium">{formatScore(match.required_coverage * 100)}</span>
                </div>
                <div>
                    <span className="text-gray-600">Preferred Coverage:</span>{' '}
                    <span className="font-medium">{formatScore(match.preferred_coverage * 100)}</span>
                </div>
                <div>
                    <span className="text-gray-600">Matched Requirements:</span>{' '}
                    <span className="font-medium">
                        {match.matched_requirements_count} / {match.total_requirements}
                    </span>
                </div>
                <div>
                    <span className="text-gray-600">Penalties:</span>{' '}
                    <span className="font-medium">{match.penalties.toFixed(1)}</span>
                </div>
            </div>
        </section>
    );
}

function RequirementCard({ req }: { req: any }) {
    const isRequired = req.req_type === 'required';
    const baseClass = isRequired
        ? req.is_covered
            ? 'bg-blue-50 border-blue-200'
            : 'bg-red-50 border-red-200'
        : req.is_covered
          ? 'bg-amber-50 border-amber-200'
          : 'bg-gray-50 border-gray-200';

    return (
        <div className={`p-3 rounded-lg border ${baseClass}`}>
            <div className="flex items-start justify-between gap-2">
                <div className="flex-1">
                    <div className="flex items-center gap-2 mb-1">
                        <Badge
                            variant={isRequired ? 'default' : 'info'}
                            className={isRequired ? 'bg-blue-100 text-blue-800 border-blue-200' : ''}
                        >
                            {isRequired ? 'Required' : 'Preferred'}
                        </Badge>
                    </div>
                    <div className="font-medium text-sm mb-1">
                        {req.requirement_text || 'No description'}
                    </div>

                    {req.evidence_text ? (
                        <div className="text-xs text-gray-600 mt-1">
                            <span className="font-medium">Evidence:</span> {req.evidence_text}
                            {req.evidence_section ? (
                                <span className="text-gray-500"> (from {req.evidence_section})</span>
                            ) : null}
                        </div>
                    ) : null}
                </div>

                <div className="flex flex-col items-end gap-1">
                    <Badge variant={req.is_covered ? 'success' : 'error'}>
                        {req.is_covered ? '✓ Covered' : '✗ Missing'}
                    </Badge>
                    <span className="text-xs text-gray-500">
                        {(req.similarity_score * 100).toFixed(0)}% semantic similarity
                    </span>
                </div>
            </div>
        </div>
    );
}

function RequirementsSection({ requirements }: { requirements: any[] }) {
    const requiredReqs = requirements.filter((req) => req.req_type === 'required');
    const preferredReqs = requirements.filter((req) => req.req_type === 'preferred');

    const requiredCovered = requiredReqs.filter((req) => req.is_covered).length;
    const preferredCovered = preferredReqs.filter((req) => req.is_covered).length;

    return (
        <section>
            <h4 className="font-semibold mb-3">Requirements ({requirements.length})</h4>

            {requiredReqs.length > 0 && (
                <div className="mb-4">
                    <div className="flex items-center justify-between mb-2">
                        <h5 className="text-sm font-medium text-gray-700">Required</h5>
                        <span className="text-xs text-gray-500">
                            {requiredCovered}/{requiredReqs.length} covered
                        </span>
                    </div>
                    <div className="space-y-2">
                        {requiredReqs.map((req) => (
                            <RequirementCard key={req.requirement_id} req={req} />
                        ))}
                    </div>
                </div>
            )}

            {preferredReqs.length > 0 && (
                <div>
                    <div className="flex items-center justify-between mb-2">
                        <h5 className="text-sm font-medium text-gray-700">Preferred</h5>
                        <span className="text-xs text-gray-500">
                            {preferredCovered}/{preferredReqs.length} covered
                        </span>
                    </div>
                    <div className="space-y-2">
                        {preferredReqs.map((req) => (
                            <RequirementCard key={req.requirement_id} req={req} />
                        ))}
                    </div>
                </div>
            )}
        </section>
    );
}

function JobDescriptionSection({ description }: { description: string }) {
    return (
        <section>
            <h4 className="font-semibold mb-3">Job Description</h4>
            <div className="prose prose-sm max-w-none">
                <p className="text-gray-700 whitespace-pre-wrap">{description}</p>
            </div>
        </section>
    );
}

function ModalBody({ isLoading, data }: { isLoading: boolean; data: any }) {
    if (isLoading) return <LoadingState />;
    if (!data) return <ErrorState message="Failed to load match details" />;

    return (
        <div className="space-y-6">
            <JobInfoSection job={data.job} />
            <ScoresSection match={data.match} />
            <RequirementsSection requirements={data.requirements} />
            {data.job.description ? <JobDescriptionSection description={data.job.description} /> : null}
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
