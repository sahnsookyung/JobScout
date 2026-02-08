import React from 'react';
import { MapPin, Building2, Laptop, TrendingUp } from 'lucide-react';
import type { MatchSummary } from '@/types/api';
import { Badge } from '@/components/ui/Badge';
import { Card } from '@/components/ui/Card';
import { formatScore, getScoreBadgeColor } from '@/utils/formatters';

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
    return (
        <Card
            className="p-5 hover:border-blue-400 hover:shadow-lg transition-all duration-200 cursor-pointer group bg-white"
            onClick={() => onSelect(match.match_id)}
        >
            <div className="flex justify-between items-start mb-4">
                <div className="flex-1">
                    <h3 className="text-lg font-semibold text-gray-900 mb-2 group-hover:text-blue-600 transition-colors">
                        {match.title}
                    </h3>
                    <div className="flex items-center gap-4 text-sm text-gray-600">
                        <div className="flex items-center gap-1.5">
                            <Building2 className="w-4 h-4 text-gray-400" />
                            <span className="font-medium">{match.company}</span>
                        </div>
                        {match.location && (
                            <div className="flex items-center gap-1.5">
                                <MapPin className="w-4 h-4 text-gray-400" />
                                <span>{match.location}</span>
                            </div>
                        )}
                        {match.is_remote && (
                            <Badge variant="info" className="flex items-center gap-1">
                                <Laptop className="w-3 h-3" />
                                Remote
                            </Badge>
                        )}
                    </div>
                </div>

                {/* Overall score prominently displayed */}
                <div className="flex flex-col items-end ml-4">
                    <div className="flex items-center gap-1 text-2xl font-bold text-blue-600">
                        <TrendingUp className="w-5 h-5" />
                        {formatScore(match.overall_score)}
                    </div>
                    <span className="text-xs text-gray-500">Overall</span>
                </div>
            </div>

            <div className="flex gap-2 flex-wrap">
                <Badge className={`${getScoreBadgeColor(match.fit_score || 0)} font-medium`}>
                    Fit: {formatScore(match.fit_score)}
                </Badge>
                {showWantScore && match.want_score !== null && (
                    <Badge className={`${getScoreBadgeColor(match.want_score)} font-medium`}>
                        Want: {formatScore(match.want_score)}
                    </Badge>
                )}
                <Badge variant="default" className="bg-purple-100 text-purple-800 font-medium">
                    Req: {formatScore(match.required_coverage * 100)}
                </Badge>
            </div>

            <div className="mt-3 pt-3 border-t border-gray-100 flex justify-between items-center">
                <span className="text-xs text-gray-500 capitalize">
                    {match.match_type.replace('_', ' ')}
                </span>
                <span className="text-xs text-blue-600 font-medium group-hover:underline">
                    View Details →
                </span>
            </div>
        </Card>
    );
};
