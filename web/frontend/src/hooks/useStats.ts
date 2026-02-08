import { useQuery } from '@tanstack/react-query';
import { matchesApi } from '@/services/matchesApi';

export const useStats = () => {
    return useQuery({
        queryKey: ['stats'],
        queryFn: async () => {
            const response = await matchesApi.getStats();
            return response.data.stats;
        },
        staleTime: 60000,
    });
};
