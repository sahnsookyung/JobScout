import { useQuery } from '@tanstack/react-query';
import { matchesApi, type GetStatsParams } from '@/services/matchesApi';

export const useStats = (params: GetStatsParams = {}) => {
    return useQuery({
        queryKey: ['stats', params],
        queryFn: async () => {
            const response = await matchesApi.getStats(params);
            return response.data.stats;
        },
        staleTime: 60000,
        refetchOnWindowFocus: false,
    });
};
