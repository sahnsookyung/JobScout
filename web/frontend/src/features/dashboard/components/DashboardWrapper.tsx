import React from 'react';

export const DashboardWrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
    <div className="relative bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-50 rounded-3xl overflow-hidden">
        <div className="absolute top-0 right-0 w-64 h-64 bg-blue-400/10 rounded-full blur-3xl" />
        <div className="absolute bottom-0 left-0 w-48 h-48 bg-indigo-400/10 rounded-full blur-3xl" />
        <div className="relative p-6">{children}</div>
    </div>
);
