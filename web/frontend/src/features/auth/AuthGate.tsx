import { type ReactNode } from 'react';
import { useAuth } from './useAuth';
import { GoogleLoginScreen } from './GoogleLoginScreen';

interface AuthGateProps {
    readonly children: ReactNode;
}

export function AuthGate({ children }: AuthGateProps) {
    const clientId = import.meta.env.VITE_GOOGLE_CLIENT_ID as string | undefined;
    const authRequired = String(import.meta.env.VITE_AUTH_REQUIRED ?? '').toLowerCase() === 'true'
        || import.meta.env.PROD;

    if (!clientId) {
        if (authRequired) {
            return (
                <main className="flex min-h-screen items-center justify-center bg-canvas px-4 text-ink">
                    <div className="w-full max-w-md border border-rule bg-surface px-7 py-6">
                        <p className="caption">Configuration</p>
                        <h1 className="mt-1 text-[18px] font-medium text-ink">Sign-in is not configured</h1>
                        <p className="mt-3 text-[13px] leading-relaxed text-ink-soft" role="alert">
                            This hosted JobScout build is missing its Google web client ID.
                        </p>
                    </div>
                </main>
            );
        }
        return <>{children}</>;
    }

    return <AuthGateInner>{children}</AuthGateInner>;
}

function AuthGateInner({ children }: AuthGateProps) {
    const { user, isReady, restoreError, retrySession, logout } = useAuth();

    if (!isReady) {
        return (
            <div className="flex min-h-screen items-center justify-center bg-canvas text-ink">
                <output
                    className="flex items-center gap-3 text-[13px] text-ink-soft"
                    aria-live="polite"
                    aria-label="Restoring your session"
                >
                    <span className="relative flex h-2 w-2">
                        <span className="ember absolute inset-0 rounded-full bg-accent opacity-40" aria-hidden="true" />
                        <span className="relative m-auto h-1 w-1 rounded-full bg-accent" />
                    </span>
                    <span>Restoring your session...</span>
                </output>
            </div>
        );
    }

    if (restoreError) {
        return (
            <div className="flex min-h-screen items-center justify-center bg-canvas px-4 text-ink">
                <div className="w-full max-w-md border border-rule bg-surface enter">
                    <div className="border-b border-rule px-7 py-5">
                        <p className="caption">Session</p>
                        <h1 className="mt-1 text-[18px] font-medium text-ink">Restore didn’t complete</h1>
                    </div>
                    <div className="px-7 py-6">
                        <p className="border border-warn/40 bg-warn-soft px-3 py-2 text-[13px] text-ink" role="alert">
                            {restoreError}
                        </p>
                        <div className="mt-5 flex flex-col gap-2 sm:flex-row">
                            <button
                                type="button"
                                className="btn-accent inline-flex h-10 flex-1 items-center justify-center rounded-md text-[13px] font-medium"
                                onClick={retrySession}
                            >
                                Try again
                            </button>
                            <button
                                type="button"
                                className="inline-flex h-10 flex-1 items-center justify-center rounded-md border border-rule bg-surface px-4 text-[13px] font-medium text-ink-soft transition-colors hover:border-rule-strong hover:text-ink"
                                onClick={logout}
                            >
                                Sign out
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        );
    }

    if (!user) {
        return <GoogleLoginScreen />;
    }

    return <>{children}</>;
}
