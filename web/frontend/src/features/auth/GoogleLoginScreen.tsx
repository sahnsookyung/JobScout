import { useEffect, useRef } from 'react';
import { Briefcase } from 'lucide-react';
import { useAuth, type AuthUser } from './useAuth';

declare global {
    interface Window {
        google?: {
            accounts: {
                id: {
                    initialize: (config: object) => void;
                    renderButton: (element: HTMLElement, config: object) => void;
                };
            };
        };
    }
}

interface GoogleCredentialResponse {
    credential: string;
}

function parseJwt(token: string): Record<string, string> {
    try {
        return JSON.parse(atob(token.split('.')[1]));
    } catch {
        return {};
    }
}

export function GoogleLoginScreen() {
    const { login } = useAuth();
    const buttonRef = useRef<HTMLDivElement>(null);
    const clientId = import.meta.env.VITE_GOOGLE_CLIENT_ID as string;

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
            if (!window.google || !buttonRef.current) return;
            window.google.accounts.id.initialize({
                client_id: clientId,
                callback: (response: GoogleCredentialResponse) => {
                    const payload = parseJwt(response.credential);
                    const user: AuthUser = {
                        email: payload.email ?? '',
                        name: payload.name ?? payload.email ?? '',
                        picture: payload.picture,
                    };
                    login(user, response.credential);
                },
            });
            window.google.accounts.id.renderButton(buttonRef.current, {
                theme: 'outline',
                size: 'large',
                text: 'signin_with',
                shape: 'rectangular',
            });
        }

        // Poll until Google SDK is ready
        const interval = setInterval(() => {
            if (window.google) {
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
                    <p className="text-sm text-gray-500 mt-1">Sign in to continue</p>
                </div>
                <div ref={buttonRef} />
            </div>
        </div>
    );
}
