import { useQuery } from '@tanstack/react-query';
import { matchesApi, type GetMatchesParams } from '@/services/matchesApi';

export const useMatches = (params: GetMatchesParams = {}) => {
    return useQuery({
        queryKey: ['matches', params],
        queryFn: async () => {
            const response = await matchesApi.getMatches(params);
            return response.data;
        },
        staleTime: 30000, // 30 seconds
        refetchOnWindowFocus: false,
    });
};
