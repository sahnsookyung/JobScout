import { useState } from 'react';
import { AuthGate } from '@/features/auth/AuthGate';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MatchList } from '@/features/matches/components/MatchList';
import { MatchDetailsModal } from '@/features/matches/components/MatchDetailsModal';
import { PolicyPanel } from '@/features/config/components/PolicyPanel';
import { DashboardControls } from '@/features/dashboard/components/DashboardControls';
import { DashboardHeader } from '@/features/dashboard/components/DashboardHeader';
import { ToastProvider } from '@/components/ui/Toast';

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
        <div className="min-h-screen bg-gradient-to-br from-gray-50 via-blue-50 to-gray-50">
            <DashboardHeader />

            {/* Main Content */}
            <main className="max-w-[1800px] mx-auto px-4 sm:px-6 lg:px-8 py-6">
                {/* Compact Controls Bar at Top */}
                <div className="mb-6">
                    <DashboardControls />
                </div>

                {/* Two Column Layout: Matches + Sidebar */}
                <div className="grid grid-cols-1 xl:grid-cols-main-sidebar gap-6">
                    {/* Panels: show first on small screens, right column on xl+ */}
                    <aside className="order-first xl:order-last space-y-6">
                        <PolicyPanel />
                    </aside>

                    {/* Matches: show after panels on small screens, left column on xl+ */}
                    <section className="order-last xl:order-first min-w-0">
                        <MatchList onMatchSelect={setSelectedMatchId} />
                    </section>
                </div>
            </main>

            {/* Match Details Modal */}
            <MatchDetailsModal
                matchId={selectedMatchId}
                onClose={() => setSelectedMatchId(null)}
            />
        </div>
    );
}

function App() {
    return (
        <QueryClientProvider client={queryClient}>
            <ToastProvider />
            <AuthGate>
                <AppContent />
            </AuthGate>
        </QueryClientProvider>
    );
}

export default App;
