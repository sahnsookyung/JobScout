import React from 'react';

import { usePolicy } from '@/hooks/usePolicy';
import { useStats } from '@/hooks/useStats';
import { POLICY_PRESET_VALUES } from '@/utils/constants';
import { FetchSourcesPanel } from './FetchSourcesPanel';
import { JobInventoryPanel } from './JobInventoryPanel';

export const JobManagementPanel: React.FC = () => {
    const { policy } = usePolicy();
    const effectivePolicy = policy ?? POLICY_PRESET_VALUES.balanced;
    const { data: stats } = useStats({
        min_fit: effectivePolicy.min_fit,
        top_k: effectivePolicy.top_k,
    });

    return (
        <div className="space-y-8">
            <FetchSourcesPanel />
            <JobInventoryPanel stats={stats} />
        </div>
    );
};
