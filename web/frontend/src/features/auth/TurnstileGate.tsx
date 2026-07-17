import { useEffect, useRef, useState } from 'react';

import {
    clearTurnstileVerification,
    hasTurnstileVerification,
    storeTurnstileVerification,
    TURNSTILE_RESET_EVENT,
} from '@/utils/turnstile';

declare global {
    interface Window {
        turnstile?: {
            render: (element: HTMLElement, options: Record<string, unknown>) => string;
            remove: (widgetId: string) => void;
        };
    }
}

export function TurnstileGate() {
    const siteKey = String(import.meta.env.VITE_TURNSTILE_SITE_KEY ?? '').trim();
    const containerRef = useRef<HTMLDivElement>(null);
    const [verified, setVerified] = useState(hasTurnstileVerification);

    useEffect(() => {
        const reset = () => setVerified(false);
        window.addEventListener(TURNSTILE_RESET_EVENT, reset);
        return () => window.removeEventListener(TURNSTILE_RESET_EVENT, reset);
    }, []);

    useEffect(() => {
        if (!siteKey || verified) return undefined;
        const scriptId = 'cloudflare-turnstile';
        let script = document.getElementById(scriptId) as HTMLScriptElement | null;
        if (!script) {
            script = document.createElement('script');
            script.id = scriptId;
            script.src = 'https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit';
            script.async = true;
            script.defer = true;
            document.head.appendChild(script);
        }

        let widgetId: string | null = null;
        const interval = window.setInterval(() => {
            if (!window.turnstile || !containerRef.current || widgetId) return;
            widgetId = window.turnstile.render(containerRef.current, {
                sitekey: siteKey,
                action: 'jobscout-expensive-operation',
                callback: (token: string) => {
                    storeTurnstileVerification(token);
                    setVerified(true);
                },
                'expired-callback': () => {
                    clearTurnstileVerification();
                    setVerified(false);
                },
                'error-callback': () => {
                    clearTurnstileVerification();
                    setVerified(false);
                },
            });
        }, 100);
        return () => {
            window.clearInterval(interval);
            if (widgetId && window.turnstile) window.turnstile.remove(widgetId);
        };
    }, [siteKey, verified]);

    if (!siteKey || verified) return null;
    return (
        <section className="mb-6 border border-rule bg-surface px-4 py-3" aria-label="Security check">
            <p className="mb-2 text-[13px] text-ink-soft">
                Complete this one-time security check before uploading or running AI features.
            </p>
            <div ref={containerRef} />
        </section>
    );
}
