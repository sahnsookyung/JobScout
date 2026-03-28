import React from 'react';
import { MapPin, Building2, Laptop, Eye, EyeOff, ArrowUpRight, Award } from 'lucide-react';
import type { MatchSummary } from '@/types/api';
import { formatScore } from '@/utils/formatters';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { matchesApi } from '@/services/matchesApi';
import { toast } from '@/components/ui/Toast';

interface MatchCardProps {
    match: MatchSummary;
    onSelect: (matchId: string) => void;
    showWantScore?: boolean;
}

export const MatchCard: React.FC<MatchCardProps> = ({
    match,
    onSelect,
    showWantScore = false,
}) => {
    const queryClient = useQueryClient();

    const toggleHiddenMutation = useMutation({
        mutationFn: (matchId: string) => matchesApi.toggleHidden(matchId),
        onSuccess: (response, matchId) => {
            const newlyHidden = response?.data?.is_hidden;
            if (typeof newlyHidden !== 'boolean') {
                toast.error('Failed to update job visibility');
                return;
            }

            queryClient.setQueryData(['matches'], (old: any) => {
                if (!old?.matches) return old;
                return {
                    ...old,
                    matches: old.matches.map((m: MatchSummary) =>
                        m.match_id === matchId ? { ...m, is_hidden: newlyHidden } : m
                    ),
                };
            });

            queryClient.invalidateQueries({ queryKey: ['matches'] });
            queryClient.invalidateQueries({ queryKey: ['stats'] });

            if (newlyHidden) {
                toast.success('Job hidden', {
                    action: {
                        label: 'Undo',
                        onClick: () => {
                            toggleHiddenMutation.mutate(matchId);
                        },
                    },
                    duration: 5000,
                });
            }
        },
        onError: (error) => {
            console.error('Failed to toggle hidden status:', error);
            toast.error('Failed to update job visibility');
        },
    });

    const handleToggleHidden = (e: React.MouseEvent) => {
        e.stopPropagation();
        toggleHiddenMutation.mutate(match.match_id);
    };

    const isHighScore = match.overall_score >= 80;
    const isMediumScore = match.overall_score >= 60 && match.overall_score < 80;
    let cardClasses = 'border-gray-200 hover:border-blue-300';
    if (match.is_hidden) {
        cardClasses = 'opacity-50 border-gray-200';
    } else if (isHighScore) {
        cardClasses = 'border-transparent bg-gradient-to-br from-blue-50 via-indigo-50 to-purple-50 hover:border-blue-300';
    }

    let scoreBadgeGradient = 'from-gray-400 to-gray-500';
    if (isHighScore) {
        scoreBadgeGradient = 'from-blue-500 via-indigo-500 to-purple-500';
    } else if (isMediumScore) {
        scoreBadgeGradient = 'from-blue-400 to-indigo-400';
    }

    const hideButtonClasses = match.is_hidden
        ? 'bg-gray-200/80 text-gray-600 hover:bg-gray-300/80'
        : 'bg-white/80 text-gray-600 hover:bg-white hover:shadow-lg';

    return (
        <article
            className={`group relative bg-white rounded-3xl p-8 cursor-pointer transition-all duration-300 hover:scale-[1.02] hover:shadow-2xl border-2 overflow-hidden ${cardClasses}`}
        >
            <button
                type="button"
                className="absolute inset-0 z-10 rounded-3xl focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 focus-visible:ring-offset-2 focus-visible:ring-offset-white"
                onClick={() => onSelect(match.match_id)}
                aria-label={`View details for ${match.title} at ${match.company}`}
            />

            {/* Score Badge Container */}
            <div className="absolute top-6 right-6 z-20 flex flex-col items-end gap-2">
                {/* Score Badge */}
                <div className={`w-20 h-20 rounded-xl bg-gradient-to-br shadow-md flex flex-col items-center justify-center ${scoreBadgeGradient}`}>
                    <div className="text-2xl font-bold text-white leading-none">
                        {formatScore(match.overall_score)}
                    </div>
                    <div className="text-[8px] font-medium text-white/70 uppercase tracking-wider mt-0.5">
                        Score
                    </div>
                </div>

                {/* Top Match Badge - Now below score */}
                {isHighScore && !match.is_hidden && (
                    <div className="flex items-center gap-1.5 px-3 py-1.5 bg-gradient-to-r from-yellow-400 to-orange-400 text-white rounded-lg font-black text-xs shadow-lg">
                        <Award className="w-3.5 h-3.5" aria-hidden="true" />
                        <span>Top Match</span>
                    </div>
                )}

                {/* Hide Button - positioned below the score badges */}
                <button
                    type="button"
                    onClick={handleToggleHidden}
                    disabled={toggleHiddenMutation.isPending}
                    className={`mt-0.5 p-2.5 rounded-xl transition-all duration-200 backdrop-blur-sm disabled:opacity-50 disabled:cursor-not-allowed ${hideButtonClasses}`}
                    title={match.is_hidden ? 'Unhide this job' : 'Hide this job'}
                    aria-label={match.is_hidden ? 'Unhide this job' : 'Hide this job'}
                    aria-pressed={match.is_hidden}
                >
                    {toggleHiddenMutation.isPending ? (
                        <div className="w-4 h-4 border-2 border-current border-t-transparent rounded-full animate-spin" />
                    ) : match.is_hidden ? (
                        <EyeOff className="w-4 h-4" />
                    ) : (
                        <Eye className="w-4 h-4" />
                    )}
                </button>
            </div>

            {/* Content */}
            <div className="relative z-0 pr-28">
                <h3 className="text-2xl font-black text-gray-900 mb-4 group-hover:text-blue-600 transition-colors leading-tight">
                    {match.title}
                </h3>

                <div className="flex flex-wrap items-center gap-4 mb-6">
                    <div className="flex items-center gap-2 px-3 py-1.5 bg-gray-100 rounded-lg">
                        <Building2 className="w-4 h-4 text-gray-500" aria-hidden="true" />
                        <span className="font-bold text-gray-900 text-sm">{match.company}</span>
                    </div>
                    {match.location && (
                        <div className="flex items-center gap-2 text-gray-600">
                            <MapPin className="w-4 h-4" aria-hidden="true" />
                            <span className="text-sm font-medium">{match.location}</span>
                        </div>
                    )}
                    {match.is_remote && (
                        <div className="flex items-center gap-1.5 px-3 py-1.5 bg-gradient-to-r from-blue-500 to-indigo-500 text-white rounded-lg font-bold text-xs shadow-md">
                            <Laptop className="w-3.5 h-3.5" aria-hidden="true" />
                            <span>Remote</span>
                        </div>
                    )}
                    {match.is_hidden && (
                        <div className="px-3 py-1.5 bg-gray-200 text-gray-700 rounded-lg font-bold text-xs">
                            Hidden
                        </div>
                    )}
                </div>

                {/* Visual Score Bars */}
                <div className="space-y-3 mb-6">
                    <ScoreBar label="Fit Match" value={match.fit_score || 0} gradient="from-blue-500 to-blue-600" />
                    {showWantScore && match.want_score !== null && match.want_score !== undefined && (
                        <ScoreBar label="Want Match" value={match.want_score} gradient="from-indigo-500 to-purple-500" />
                    )}
                    <ScoreBar label="Requirements" value={match.required_coverage * 100} gradient="from-blue-400 to-indigo-400" />
                </div>

                <div className="flex items-center justify-between pt-4 border-t-2 border-gray-100">
                    <span className="text-xs font-bold text-gray-500 uppercase tracking-wider">
                        {match.match_type.replace('_', ' ')}
                    </span>
                    <div className="flex items-center gap-2 text-blue-600 font-bold group-hover:gap-3 transition-all">
                        <span className="text-sm">View Details</span>
                        <ArrowUpRight className="w-4 h-4 group-hover:translate-x-0.5 group-hover:-translate-y-0.5 transition-transform" aria-hidden="true" />
                    </div>
                </div>
            </div>
        </article>
    );
};

interface ScoreBarProps {
    label: string;
    value: number;
    gradient: string;
}

const ScoreBar: React.FC<ScoreBarProps> = ({ label, value, gradient }) => (
    <div>
        <div className="flex justify-between items-center mb-1.5">
            <span className="text-xs font-bold text-gray-600 uppercase tracking-wide">{label}</span>
            <span className="text-sm font-black text-gray-900">{formatScore(value)}</span>
        </div>
        <div className="h-2 bg-gray-200 rounded-full overflow-hidden">
            <div
                className={`h-full bg-gradient-to-r ${gradient} rounded-full transition-all duration-1000 ease-out`}
                style={{ width: `${Math.min(100, Math.max(0, value))}%` }}
            />
        </div>
    </div>
);
