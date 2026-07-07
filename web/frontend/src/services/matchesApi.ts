import { apiClient } from './api';
import type {
    MatchesResponse,
    MatchDetailResponse,
    MatchExplanationResponse,
    MatchLlmEvaluationListResponse,
    MatchLlmEvaluationMutationResponse,
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
    tier?: 'primary' | 'all';
    limit?: number;
    offset?: number;
    cursor?: string | null;
    page_mode?: 'offset' | 'cursor';
    view?: 'summary' | 'compact';
    include?: 'llm' | string;
    llm_ordering?: boolean;
}

export interface GetStatsParams {
    min_fit?: number;
    top_k?: number;
}

export const matchesApi = {
    getMatches: (params: GetMatchesParams = {}) =>
        apiClient.get<MatchesResponse>('/matches', { params }),

    getMatchDetails: (matchId: string) =>
        apiClient.get<MatchDetailResponse>(`/matches/${matchId}`),

    getMatchExplanation: (matchId: string) =>
        apiClient.get<MatchExplanationResponse>(`/matches/${matchId}/explanation`),

    getLlmEvaluations: (matchId: string) =>
        apiClient.get<MatchLlmEvaluationListResponse>(`/matches/${matchId}/llm-evaluations`),

    generateLlmEvaluation: (matchId: string, force = false) =>
        apiClient.post<MatchLlmEvaluationMutationResponse>(
            `/matches/${matchId}/llm-evaluations`,
            { force },
        ),

    deleteLlmEvaluation: (matchId: string, evaluationId: string) =>
        apiClient.delete<MatchLlmEvaluationMutationResponse>(
            `/matches/${matchId}/llm-evaluations/${evaluationId}`,
        ),

    retryLlmEvaluation: (matchId: string, evaluationId: string) =>
        apiClient.post<MatchLlmEvaluationMutationResponse>(
            `/matches/${matchId}/llm-evaluations/${evaluationId}/retry`,
        ),

    getStats: (params: GetStatsParams = {}) =>
        apiClient.get<StatsResponse>('/stats', { params }),

    getMatchSummary: (params: GetStatsParams = {}) =>
        apiClient.get<StatsResponse>('/matches/summary', { params }),

    toggleHidden: (matchId: string) =>
        apiClient.post<{ success: boolean; match_id: string; is_hidden: boolean }>(`/matches/${matchId}/hide`),
};
