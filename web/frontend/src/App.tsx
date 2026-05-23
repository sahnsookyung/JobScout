import { useState } from 'react';
import { AuthGate } from '@/features/auth/AuthGate';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MatchList } from '@/features/matches/components/MatchList';
import { MatchDetailsModal } from '@/features/matches/components/MatchDetailsModal';
import { EmailVerificationPage } from '@/features/notifications/components/EmailVerificationPage';
import { PolicyPanel } from '@/features/config/components/PolicyPanel';
import { DashboardControls } from '@/features/dashboard/components/DashboardControls';
import { DashboardHeader } from '@/features/dashboard/components/DashboardHeader';
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

function AppContent() {
    const [selectedMatchId, setSelectedMatchId] = useState<string | null>(null);

    return (
        <div className="min-h-screen bg-canvas text-ink">
            <DashboardHeader />

            <main className="mx-auto max-w-[var(--container-content)] px-5 pb-24 pt-8 sm:px-8 lg:px-10">
                <div className="enter mb-10">
                    <DashboardControls />
                </div>

                <div className="grid grid-cols-1 gap-10 lg:grid-cols-[280px_minmax(0,1fr)]">
                    <aside className="order-first lg:order-first">
                        <div className="sticky top-24">
                            <PolicyPanel />
                        </div>
                    </aside>

                    <section className="order-last lg:order-last min-w-0">
                        <MatchList onMatchSelect={setSelectedMatchId} />
                    </section>
                </div>
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
