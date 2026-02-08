import React, { useState, useEffect } from 'react';
import { useMatches } from '@/hooks/useMatches';
import { MatchCard } from './MatchCard';
import { MatchFilters } from './MatchFilters';
import type { MatchStatus, SortBy } from '@/types/api';

interface MatchListProps {
    onMatchSelect: (matchId: string) => void;
}

export const MatchList: React.FC<MatchListProps> = ({ onMatchSelect }) => {
    const [status, setStatus] = useState<MatchStatus>('active');
    const [remoteOnly, setRemoteOnly] = useState(false);
    const [showWantScore, setShowWantScore] = useState(false);
    const [sortBy, setSortBy] = useState<SortBy>('overall');
    const [showHidden, setShowHidden] = useState(() => {
        const saved = localStorage.getItem('jobscout_show_hidden');
        return saved === 'true';
    });

    // Persist showHidden preference
    useEffect(() => {
        localStorage.setItem('jobscout_show_hidden', showHidden.toString());
    }, [showHidden]);

    const { data, isLoading, error, refetch } = useMatches({
        status,
        remote_only: remoteOnly,
        show_hidden: showHidden,
    });

    // Client-side sorting
    const sortedMatches = React.useMemo(() => {
        if (!data?.matches) return [];

        const matches = [...data.matches];

        if (sortBy === 'fit') {
            return matches.sort((a, b) => (b.fit_score || 0) - (a.fit_score || 0));
        } else if (sortBy === 'want') {
            return matches.sort((a, b) => (b.want_score || 0) - (a.want_score || 0));
        } else {
            return matches.sort((a, b) => b.overall_score - a.overall_score);
        }
    }, [data?.matches, sortBy]);

    if (isLoading) {
        return (
            <div className="flex items-center justify-center h-64">
                <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600" />
            </div>
        );
    }

    if (error) {
        return (
            <div className="text-center text-red-600 p-8">
                <p>Error loading matches. Please try again.</p>
                <button
                    onClick={() => refetch()}
                    className="mt-4 text-blue-600 hover:underline"
                >
                    Retry
                </button>
            </div>
        );
    }

    return (
        <div className="space-y-4">
            <MatchFilters
                status={status}
                onStatusChange={setStatus}
                remoteOnly={remoteOnly}
                onRemoteOnlyChange={setRemoteOnly}
                showWantScore={showWantScore}
                onShowWantScoreChange={setShowWantScore}
                sortBy={sortBy}
                onSortByChange={setSortBy}
                showHidden={showHidden}
                onShowHiddenChange={setShowHidden}
            />

            <div className="text-sm text-gray-600">
                Showing {sortedMatches.length} match{sortedMatches.length !== 1 ? 'es' : ''}
            </div>

            <div className="grid grid-cols-1 gap-4">
                {sortedMatches.map((match) => (
                    <MatchCard
                        key={match.match_id}
                        match={match}
                        onSelect={onMatchSelect}
                        showWantScore={showWantScore}
                    />
                ))}
            </div>

            {sortedMatches.length === 0 && (
                <div className="text-center text-gray-500 py-12">
                    No matches found with current filters.
                </div>
            )}
        </div>
    );
};
