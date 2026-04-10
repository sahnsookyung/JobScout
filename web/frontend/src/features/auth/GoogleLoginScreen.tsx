import { useEffect, useRef, useState } from 'react';
import { Briefcase } from 'lucide-react';
import { cloudAuthApi } from '@/services/cloudAuthApi';
import { useAuth } from './useAuth';

declare global {
    // eslint-disable-next-line no-var
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

export function GoogleLoginScreen() {
    const { login } = useAuth();
    const buttonRef = useRef<HTMLDivElement>(null);
    const clientId = import.meta.env.VITE_GOOGLE_CLIENT_ID as string;
    const [isSigningIn, setIsSigningIn] = useState(false);
    const [authError, setAuthError] = useState<string | null>(null);

    useEffect(() => {
        const scriptId = 'google-gsi';
        if (!document.getElementById(scriptId)) {
            const script = document.createElement('script');
            script.id = scriptId;
            script.src = 'https://accounts.google.com/gsi/client';
            script.async = true;
            script.defer = true;
            document.head.appendChild(script);
        }

        function initButton() {
            if (!globalThis.google || !buttonRef.current) return;
            globalThis.google.accounts.id.initialize({
                client_id: clientId,
                callback: async (response: GoogleCredentialResponse) => {
                    setIsSigningIn(true);
                    setAuthError(null);
                    try {
                        const exchange = await cloudAuthApi.exchangeGoogleCredential(
                            response.credential
                        );
                        const { user, access_token: accessToken } = exchange.data;
                        login(
                            {
                                email: user.email,
                                name: user.name,
                                picture: user.picture ?? undefined,
                            },
                            accessToken
                        );
                    } catch {
                        setAuthError('Sign-in failed. Please try again.');
                    } finally {
                        setIsSigningIn(false);
                    }
                },
            });
            globalThis.google.accounts.id.renderButton(buttonRef.current, {
                theme: 'outline',
                size: 'large',
                text: 'signin_with',
                shape: 'rectangular',
            });
        }

        // Poll until Google SDK is ready
        const interval = setInterval(() => {
            if (globalThis.google) {
                clearInterval(interval);
                initButton();
            }
        }, 100);

        return () => clearInterval(interval);
    }, [clientId, login]);

    return (
        <div className="min-h-screen bg-gradient-to-br from-gray-50 via-blue-50 to-gray-50 flex items-center justify-center">
            <div className="bg-white rounded-2xl shadow-lg p-10 flex flex-col items-center gap-6 max-w-sm w-full mx-4">
                <div className="p-3 bg-blue-600 rounded-xl">
                    <Briefcase className="w-8 h-8 text-white" />
                </div>
                <div className="text-center">
                    <h1 className="text-2xl font-bold text-gray-900">JobScout</h1>
                    <p className="text-sm text-gray-500 mt-1">
                        Continue with Google to create an account or sign in
                    </p>
                </div>
                <div ref={buttonRef} />
                {isSigningIn ? (
                    <p className="text-sm text-blue-600">Finishing sign-in...</p>
                ) : null}
                {authError ? (
                    <p className="text-sm text-red-600" role="alert">
                        {authError}
                    </p>
                ) : null}
            </div>
        </div>
    );
}
