import { useQuery } from '@tanstack/react-query';
import { jobsApi, type GetJobsParams } from '@/services/jobsApi';

export const useJobs = (params: GetJobsParams = {}, enabled = true) => {
    return useQuery({
        queryKey: ['jobs', params],
        queryFn: async () => {
            const response = await jobsApi.getJobs(params);
            return response.data;
        },
        enabled,
        staleTime: 30000,
        refetchOnWindowFocus: false,
    });
};
