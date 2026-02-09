import { useState } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MatchList } from '@/features/matches/components/MatchList';
import { MatchDetailsModal } from '@/features/matches/components/MatchDetailsModal';
import { PolicyPanel } from '@/features/config/components/PolicyPanel';
import { StatsPanel } from '@/features/stats/components/StatsPanel';
import { CompactControls } from '@/features/dashboard/components/CompactControls';
import { RefreshCw, Briefcase } from 'lucide-react';
import { Button } from '@/components/ui/Button';
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

    const handleRefresh = () => {
        queryClient.invalidateQueries({ queryKey: ['matches'] });
        queryClient.invalidateQueries({ queryKey: ['stats'] });
    };

    return (
        <div className="min-h-screen bg-gradient-to-br from-gray-50 via-blue-50 to-gray-50">
            {/* Header */}
            <header className="bg-white shadow-md border-b border-gray-200 sticky top-0 z-50">
                <div className="max-w-[1800px] mx-auto px-4 sm:px-6 lg:px-8 py-4">
                    <div className="flex items-center justify-between">
                        <div className="flex items-center gap-3">
                            <div className="p-2 bg-blue-600 rounded-lg">
                                <Briefcase className="w-6 h-6 text-white" />
                            </div>
                            <div>
                                <h1 className="text-2xl font-bold text-gray-900">
                                    JobScout Dashboard
                                </h1>
                                <p className="text-sm text-gray-600">AI-Powered Job Matching</p>
                            </div>
                        </div>
                        <Button onClick={handleRefresh} variant="ghost" size="sm">
                            <RefreshCw className="w-4 h-4 mr-2" />
                            Refresh
                        </Button>
                    </div>
                </div>
            </header>

            {/* Main Content */}
            <main className="max-w-[1800px] mx-auto px-4 sm:px-6 lg:px-8 py-6">
                {/* Compact Controls Bar at Top */}
                <div className="mb-6">
                    <CompactControls />
                </div>

                {/* Two Column Layout: Matches + Sidebar */}
                <div className="grid grid-cols-1 xl:grid-cols-[1fr_400px] gap-6">
                    {/* Panels: show first on small screens, right column on xl+ */}
                    <aside className="order-first xl:order-last space-y-6">
                        {/* <StatsPanel /> */}
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
            <AppContent />
        </QueryClientProvider>
    );
}

export default App;
