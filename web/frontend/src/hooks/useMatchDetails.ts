import { useQuery } from '@tanstack/react-query';
import { matchesApi } from '@/services/matchesApi';

export const useMatchDetails = (matchId: string | null) => {
    return useQuery({
        queryKey: ['match', matchId],
        queryFn: async () => {
            if (!matchId) throw new Error('Match ID required');
            const response = await matchesApi.getMatchDetails(matchId);
            return response.data;
        },
        enabled: !!matchId,
        staleTime: 60000, // 1 minute
    });
};
