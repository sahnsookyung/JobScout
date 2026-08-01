import { lazy, Suspense, useEffect, useState } from 'react';
import { AuthGate } from '@/features/auth/AuthGate';
import { TurnstileGate } from '@/features/auth/TurnstileGate';
import { useAuth } from '@/features/auth/useAuth';
import { QueryClientProvider } from '@tanstack/react-query';
import { MatchList } from '@/features/matches/components/MatchList';
import { PolicyPanel } from '@/features/config/components/PolicyPanel';
import { DashboardControls } from '@/features/dashboard/components/DashboardControls';
import { DashboardHeader } from '@/features/dashboard/components/DashboardHeader';
import { ToastProvider } from '@/components/ui/Toast';
import { AppErrorBoundary } from '@/components/AppErrorBoundary';
import { stripAppBasePath } from '@/config/publicPath';
import { queryClient } from '@/services/queryClient';

const MatchDetailsModal = lazy(() => import(
    '@/features/matches/components/MatchDetailsModal'
).then((module) => ({ default: module.MatchDetailsModal })));
const EmailVerificationPage = lazy(() => import(
    '@/features/notifications/components/EmailVerificationPage'
).then((module) => ({ default: module.EmailVerificationPage })));
const JobManagementPanel = lazy(() => import(
    '@/features/dashboard/components/JobManagementPanel'
).then((module) => ({ default: module.JobManagementPanel })));

type WorkspaceTab = 'jobs' | 'management';

function workspaceTabClass(isActive: boolean): string {
    return [
        'inline-flex h-10 items-center justify-center border px-4 text-[12px] font-medium transition',
        isActive
            ? 'border-accent bg-accent-soft text-accent-ink'
            : 'border-rule bg-surface text-ink-muted hover:border-rule-strong hover:text-ink',
    ].join(' ');
}

function LazyContentFallback({
    label,
    overlay = false,
}: Readonly<{ label: string; overlay?: boolean }>) {
    const classes = overlay
        ? 'fixed inset-0 z-50 flex items-center justify-center bg-canvas/80 text-[13px] text-ink-soft'
        : 'flex min-h-32 items-center justify-center text-[13px] text-ink-soft';

    return (
        <div className={classes} role="status" aria-live="polite">
            {label}
        </div>
    );
}

function AppContent() {
    const { user } = useAuth();
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
                <TurnstileGate />
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
                    {user?.is_platform_admin && (
                        <button
                            type="button"
                            role="tab"
                            aria-selected={activeTab === 'management'}
                            className={workspaceTabClass(activeTab === 'management')}
                            onClick={() => setActiveTab('management')}
                        >
                            Job Management
                        </button>
                    )}
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
                        <Suspense fallback={<LazyContentFallback label="Loading job management…" />}>
                            <JobManagementPanel />
                        </Suspense>
                    </section>
                )}
            </main>

            {selectedMatchId ? (
                <Suspense fallback={<LazyContentFallback label="Loading match details…" overlay />}>
                    <MatchDetailsModal
                        matchId={selectedMatchId}
                        onClose={() => setSelectedMatchId(null)}
                    />
                </Suspense>
            ) : null}
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
                    <Suspense fallback={<LazyContentFallback label="Loading email verification…" />}>
                        <EmailVerificationPage />
                    </Suspense>
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
