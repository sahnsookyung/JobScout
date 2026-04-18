import React from 'react';

export const DashboardWrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
    <div className="border border-rule bg-surface">
        <div className="p-5 sm:p-7">{children}</div>
    </div>
);
