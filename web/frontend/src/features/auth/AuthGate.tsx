import { type ReactNode } from 'react';
import { useAuth } from './useAuth';
import { GoogleLoginScreen } from './GoogleLoginScreen';

interface AuthGateProps {
    children: ReactNode;
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
    const { user } = useAuth();

    if (!user) {
        return <GoogleLoginScreen />;
    }

    return <>{children}</>;
}
