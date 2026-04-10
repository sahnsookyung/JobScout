import { type ReactNode } from 'react';
import { useAuth } from './useAuth';
import { GoogleLoginScreen } from './GoogleLoginScreen';

interface AuthGateProps {
    readonly children: ReactNode;
}

/**
 * AuthGate — transparent in OSS mode; shows Google login in SaaS mode.
 *
 * If VITE_GOOGLE_CLIENT_ID is not set, renders children directly with no
 * login requirement. Set it to enable Google OAuth authentication.
 */
export function AuthGate({ children }: AuthGateProps) {
    const clientId = import.meta.env.VITE_GOOGLE_CLIENT_ID as string | undefined;

    // OSS mode: no client ID configured — pass through immediately
    if (!clientId) {
        return <>{children}</>;
    }

    // SaaS mode: require authentication
    return <AuthGateInner>{children}</AuthGateInner>;
}

function AuthGateInner({ children }: AuthGateProps) {
    const { user, isReady, restoreError, retrySession, logout } = useAuth();

    if (!isReady) {
        return (
            <div className="min-h-screen flex items-center justify-center bg-gray-50">
                <output
                    className="text-sm text-gray-600"
                    aria-live="polite"
                    aria-label="Restoring your session"
                >
                    Restoring your session...
                </output>
            </div>
        );
    }

    if (restoreError) {
        return (
            <div className="min-h-screen flex items-center justify-center bg-gray-50 px-4">
                <div className="max-w-md w-full rounded-2xl bg-white shadow-lg p-8 text-center space-y-4">
                    <h1 className="text-xl font-semibold text-gray-900">
                        Session restore failed
                    </h1>
                    <p className="text-sm text-gray-600" role="alert">
                        {restoreError}
                    </p>
                    <div className="flex flex-col sm:flex-row gap-3 justify-center">
                        <button
                            type="button"
                            className="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white"
                            onClick={retrySession}
                        >
                            Try again
                        </button>
                        <button
                            type="button"
                            className="rounded-lg border border-gray-300 px-4 py-2 text-sm font-medium text-gray-700"
                            onClick={logout}
                        >
                            Sign out
                        </button>
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
