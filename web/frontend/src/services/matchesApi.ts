import { apiClient } from './api';
import type {
    MatchesResponse,
    MatchDetailResponse,
    MatchExplanationResponse,
    MatchStatus,
    RankingMode,
    StatsResponse,
} from '@/types/api';

export interface GetMatchesParams {
    status?: MatchStatus;
    min_fit?: number;
    top_k?: number;
    remote_only?: boolean;
    show_hidden?: boolean;
    ranking_mode?: RankingMode;
}

export const matchesApi = {
    getMatches: (params: GetMatchesParams = {}) =>
        apiClient.get<MatchesResponse>('/matches', { params }),

    getMatchDetails: (matchId: string) =>
        apiClient.get<MatchDetailResponse>(`/matches/${matchId}`),

    getMatchExplanation: (matchId: string) =>
        apiClient.get<MatchExplanationResponse>(`/matches/${matchId}/explanation`),

    getStats: () => apiClient.get<StatsResponse>('/stats'),

    toggleHidden: (matchId: string) =>
        apiClient.post<{ success: boolean; match_id: string; is_hidden: boolean }>(`/matches/${matchId}/hide`),
};
