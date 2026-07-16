import { useEffect, useRef, useState } from 'react';
import { cloudAuthApi } from '@/services/cloudAuthApi';
import { useAuth } from './useAuth';

declare global {
    var google: {
        accounts: {
            id: {
                initialize: (config: object) => void;
                renderButton: (element: HTMLElement, config: object) => void;
            };
        };
    } | undefined;
}

interface GoogleCredentialResponse {
    credential: string;
}

const NONCE_REFRESH_SKEW_MS = 30_000;
const MIN_NONCE_REFRESH_DELAY_MS = 30_000;
const MAX_NONCE_REFRESH_DELAY_MS = 4 * 60_000;
const NONCE_RETRY_BASE_DELAY_MS = 1_000;
const NONCE_RETRY_MAX_DELAY_MS = 30_000;
const NONCE_RETRY_JITTER_MS = 500;
const SECURE_SIGN_IN_ERROR = 'Secure sign-in is temporarily unavailable. Please try again.';

export function GoogleLoginScreen() {
    const { login } = useAuth();
    const buttonRef = useRef<HTMLDivElement>(null);
    const exchangeAttemptRef = useRef(0);
    const isMountedRef = useRef(false);
    const clientId = import.meta.env.VITE_GOOGLE_CLIENT_ID as string;
    const [isSigningIn, setIsSigningIn] = useState(false);
    const [authError, setAuthError] = useState<string | null>(null);

    useEffect(() => {
        isMountedRef.current = true;
        let nonceRefreshTimer: ReturnType<typeof setTimeout> | null = null;
        let nonceExpiryTimer: ReturnType<typeof setTimeout> | null = null;
        let activeNonceExpiresAtMs: number | null = null;
        let nonceRetryAttempt = 0;
        const scriptId = 'google-gsi';
        if (!document.getElementById(scriptId)) {
            const script = document.createElement('script');
            script.id = scriptId;
            script.src = 'https://accounts.google.com/gsi/client';
            script.async = true;
            script.defer = true;
            document.head.appendChild(script);
        }

        async function initButton() {
            if (!globalThis.google || !buttonRef.current) return;
            let nonce: string;
            let expiresAt: number;
            try {
                const response = await cloudAuthApi.createGoogleLoginNonce();
                nonce = response.data.nonce;
                expiresAt = response.data.expires_at;
            } catch {
                if (isMountedRef.current) {
                    setAuthError(SECURE_SIGN_IN_ERROR);
                    if (
                        activeNonceExpiresAtMs === null ||
                        activeNonceExpiresAtMs <= Date.now()
                    ) {
                        buttonRef.current?.replaceChildren();
                    }
                    const exponentialDelay = Math.min(
                        NONCE_RETRY_MAX_DELAY_MS,
                        NONCE_RETRY_BASE_DELAY_MS * 2 ** Math.min(nonceRetryAttempt, 5)
                    );
                    const retryDelay = Math.min(
                        NONCE_RETRY_MAX_DELAY_MS,
                        exponentialDelay + Math.floor(Math.random() * NONCE_RETRY_JITTER_MS)
                    );
                    nonceRetryAttempt += 1;
                    if (nonceRefreshTimer) {
                        clearTimeout(nonceRefreshTimer);
                    }
                    nonceRefreshTimer = setTimeout(() => {
                        nonceRefreshTimer = null;
                        void initButton();
                    }, retryDelay);
                }
                return;
            }
            if (!isMountedRef.current) return;
            nonceRetryAttempt = 0;
            activeNonceExpiresAtMs = expiresAt * 1000;
            setAuthError((currentError) =>
                currentError === SECURE_SIGN_IN_ERROR ? null : currentError
            );
            if (nonceRefreshTimer) {
                clearTimeout(nonceRefreshTimer);
            }
            if (nonceExpiryTimer) {
                clearTimeout(nonceExpiryTimer);
            }
            const refreshDelay = Math.min(
                MAX_NONCE_REFRESH_DELAY_MS,
                Math.max(
                    MIN_NONCE_REFRESH_DELAY_MS,
                    activeNonceExpiresAtMs - Date.now() - NONCE_REFRESH_SKEW_MS
                )
            );
            nonceRefreshTimer = setTimeout(() => {
                nonceRefreshTimer = null;
                void initButton();
            }, refreshDelay);
            nonceExpiryTimer = setTimeout(() => {
                activeNonceExpiresAtMs = null;
                nonceExpiryTimer = null;
                buttonRef.current?.replaceChildren();
            }, Math.max(activeNonceExpiresAtMs - Date.now(), 0));
            // GSI keeps the most recent initialization, so renewing it also replaces
            // the callback closure that carries the expiring server-side nonce.
            globalThis.google.accounts.id.initialize({
                client_id: clientId,
                nonce,
                callback: async (response: GoogleCredentialResponse) => {
                    if (!isMountedRef.current) return;
                    const attemptId = exchangeAttemptRef.current + 1;
                    exchangeAttemptRef.current = attemptId;
                    setIsSigningIn(true);
                    setAuthError(null);
                    try {
                        const exchange = await cloudAuthApi.exchangeGoogleCredential(
                            response.credential,
                            nonce
                        );
                        if (!isMountedRef.current || attemptId !== exchangeAttemptRef.current) return;
                        const { user, access_token: accessToken, tenants = [] } = exchange.data;
                        login(
                            {
                                id: user.id,
                                email: user.email,
                                name: user.name,
                                picture: user.picture ?? undefined,
                                is_platform_admin: user.is_platform_admin,
                                data_expires_at: user.data_expires_at,
                                session_expires_at: user.session_expires_at,
                            },
                            accessToken ?? tenants
                        );
                    } catch {
                        if (!isMountedRef.current || attemptId !== exchangeAttemptRef.current) return;
                        setAuthError('Sign-in didn’t go through. Please try once more.');
                        void initButton();
                    } finally {
                        if (isMountedRef.current && attemptId === exchangeAttemptRef.current) {
                            setIsSigningIn(false);
                        }
                    }
                },
            });
            buttonRef.current.replaceChildren();
            globalThis.google.accounts.id.renderButton(buttonRef.current, {
                theme: 'outline',
                size: 'large',
                text: 'signin_with',
                shape: 'rectangular',
            });
        }

        const interval = setInterval(() => {
            if (globalThis.google) {
                clearInterval(interval);
                initButton();
            }
        }, 100);

        return () => {
            isMountedRef.current = false;
            exchangeAttemptRef.current += 1;
            clearInterval(interval);
            if (nonceRefreshTimer) {
                clearTimeout(nonceRefreshTimer);
            }
            if (nonceExpiryTimer) {
                clearTimeout(nonceExpiryTimer);
            }
        };
    }, [clientId, login]);

    return (
        <main className="flex min-h-screen items-center justify-center bg-canvas px-4 py-12 text-ink">
            <div className="w-full max-w-md border border-rule bg-surface enter">
                <div className="flex items-center gap-3 border-b border-rule px-8 py-6">
                    <span className="jobscout-mark" aria-hidden="true" />
                    <span className="flex items-baseline gap-2">
                        <span className="text-[17px] font-medium tracking-tight text-ink">JobScout</span>
                        <span className="caption">Workshop</span>
                    </span>
                </div>

                <div className="px-8 py-8">
                    <p className="caption">Welcome back</p>
                    <h1 className="mt-2 text-[26px] font-medium leading-tight tracking-tight text-ink">
                        A quiet place to find your next role.
                    </h1>
                    <p className="mt-3 max-w-sm text-[14px] leading-relaxed text-ink-soft">
                        Sign in with Google to try the complete workflow. Non-admin accounts and
                        their uploaded data are deleted four hours after the last sign-in.
                    </p>

                    <div className="mt-6 flex justify-center">
                        <div ref={buttonRef} />
                    </div>

                    {isSigningIn && (
                        <p className="mt-4 flex items-center justify-center gap-2 text-[13px] text-ink-soft" aria-live="polite">
                            <span className="relative flex h-2 w-2">
                                <span className="ember absolute inset-0 rounded-full bg-accent opacity-40" aria-hidden="true" />
                                <span className="relative m-auto h-1 w-1 rounded-full bg-accent" />
                            </span>
                            <span>Finishing sign-in</span>
                        </p>
                    )}
                    {authError && (
                        <p className="mt-4 border border-warn/40 bg-warn-soft px-3 py-2 text-[13px] text-ink" role="alert">
                            {authError}
                        </p>
                    )}
                </div>

                <div className="border-t border-rule px-8 py-4 text-[12px] text-ink-muted">
                    Temporary testing accounts are isolated from other users. Shared job data remains.
                </div>
            </div>
        </main>
    );
}
