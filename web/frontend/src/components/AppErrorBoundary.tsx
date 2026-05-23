import { Component, type ErrorInfo, type ReactNode } from 'react';

interface AppErrorBoundaryProps {
    readonly children: ReactNode;
}

interface AppErrorBoundaryState {
    readonly hasError: boolean;
}

export class AppErrorBoundary extends Component<AppErrorBoundaryProps, AppErrorBoundaryState> {
    state: AppErrorBoundaryState = { hasError: false };

    static getDerivedStateFromError(): AppErrorBoundaryState {
        return { hasError: true };
    }

    componentDidCatch(error: Error, info: ErrorInfo): void {
        if (!import.meta.env.PROD) {
            console.error('[AppErrorBoundary]', error, info.componentStack);
        }
    }

    render() {
        if (!this.state.hasError) {
            return this.props.children;
        }

        return (
            <main className="flex min-h-screen items-center justify-center bg-canvas px-4 text-ink">
                <div className="w-full max-w-md border border-rule bg-surface px-7 py-6">
                    <p className="caption">Recovery</p>
                    <h1 className="mt-1 text-[18px] font-medium text-ink">JobScout needs a refresh</h1>
                    <p className="mt-3 text-[13px] leading-relaxed text-ink-soft">
                        The app hit an unexpected state. Refreshing will reload your secure session.
                    </p>
                    <button
                        type="button"
                        className="btn-accent mt-5 inline-flex h-10 items-center justify-center rounded-md px-4 text-[13px] font-medium"
                        onClick={() => globalThis.location.reload()}
                    >
                        Refresh
                    </button>
                </div>
            </main>
        );
    }
}
