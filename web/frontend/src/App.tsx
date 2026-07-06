import { useEffect, useState } from 'react';
import { AuthGate } from '@/features/auth/AuthGate';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MatchList } from '@/features/matches/components/MatchList';
import { MatchDetailsModal } from '@/features/matches/components/MatchDetailsModal';
import { EmailVerificationPage } from '@/features/notifications/components/EmailVerificationPage';
import { PolicyPanel } from '@/features/config/components/PolicyPanel';
import { DashboardControls } from '@/features/dashboard/components/DashboardControls';
import { DashboardHeader } from '@/features/dashboard/components/DashboardHeader';
import { JobManagementPanel } from '@/features/dashboard/components/JobManagementPanel';
import { ToastProvider } from '@/components/ui/Toast';
import { AppErrorBoundary } from '@/components/AppErrorBoundary';
import { stripAppBasePath } from '@/config/publicPath';

const queryClient = new QueryClient({
    defaultOptions: {
        queries: {
            refetchOnWindowFocus: false,
            retry: 1,
        },
    },
});

type WorkspaceTab = 'jobs' | 'management';

function workspaceTabClass(isActive: boolean): string {
    return [
        'inline-flex h-10 items-center justify-center border px-4 text-[12px] font-medium transition',
        isActive
            ? 'border-accent bg-accent-soft text-accent-ink'
            : 'border-rule bg-surface text-ink-muted hover:border-rule-strong hover:text-ink',
    ].join(' ');
}

function AppContent() {
    const [selectedMatchId, setSelectedMatchId] = useState<string | null>(null);
    const [activeTab, setActiveTab] = useState<WorkspaceTab>('jobs');

    useEffect(() => {
        const openManagement = () => {
            setSelectedMatchId(null);
            setActiveTab('management');
        };
        window.addEventListener('jobscout:open-job-management', openManagement);
        return () => window.removeEventListener('jobscout:open-job-management', openManagement);
    }, []);

    return (
        <div className="min-h-screen bg-canvas text-ink">
            <DashboardHeader />

            <main className="mx-auto max-w-[var(--container-content)] px-5 pb-24 pt-8 sm:px-8 lg:px-10">
                <div className="enter mb-10">
                    <DashboardControls includeManagementSections={false} />
                </div>

                <div className="mb-6 flex flex-wrap gap-2" role="tablist" aria-label="Job workspace">
                    <button
                        type="button"
                        role="tab"
                        aria-selected={activeTab === 'jobs'}
                        className={workspaceTabClass(activeTab === 'jobs')}
                        onClick={() => setActiveTab('jobs')}
                    >
                        Jobs
                    </button>
                    <button
                        type="button"
                        role="tab"
                        aria-selected={activeTab === 'management'}
                        className={workspaceTabClass(activeTab === 'management')}
                        onClick={() => setActiveTab('management')}
                    >
                        Job Management
                    </button>
                </div>

                {activeTab === 'jobs' ? (
                    <div
                        role="tabpanel"
                        aria-label="Jobs"
                        className="grid grid-cols-1 gap-10 lg:grid-cols-[280px_minmax(0,1fr)]"
                    >
                        <aside className="order-first lg:order-first">
                            <div className="sticky top-24">
                                <PolicyPanel />
                            </div>
                        </aside>

                        <section className="order-last min-w-0 lg:order-last">
                            <MatchList onMatchSelect={setSelectedMatchId} />
                        </section>
                    </div>
                ) : (
                    <section role="tabpanel" aria-label="Job Management">
                        <JobManagementPanel />
                    </section>
                )}
            </main>

            <MatchDetailsModal
                matchId={selectedMatchId}
                onClose={() => setSelectedMatchId(null)}
            />
        </div>
    );
}

function App() {
    const isEmailVerificationRoute =
        stripAppBasePath(globalThis.location.pathname) === '/verify-email';

    return (
        <QueryClientProvider client={queryClient}>
            <AppErrorBoundary>
                <ToastProvider />
                {isEmailVerificationRoute ? (
                    <EmailVerificationPage />
                ) : (
                    <AuthGate>
                        <AppContent />
                    </AuthGate>
                )}
            </AppErrorBoundary>
        </QueryClientProvider>
    );
}

export default App;
