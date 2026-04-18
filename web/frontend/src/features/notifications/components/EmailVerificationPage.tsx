import { useEffect, useState } from 'react';
import { CheckCircle2, TriangleAlert } from 'lucide-react';

import { useNotificationSettings } from '@/hooks/useNotificationSettings';

type VerificationState = 'verifying' | 'success' | 'error';

export function EmailVerificationPage() {
    const { verifyEmailOverride } = useNotificationSettings();
    const [state, setState] = useState<VerificationState>('verifying');
    const [message, setMessage] = useState('Verifying your email override...');

    useEffect(() => {
        const params = new URLSearchParams(globalThis.location.search);
        const token = params.get('token');
        if (!token) {
            setState('error');
            setMessage('Verification token is missing.');
            return;
        }

        void verifyEmailOverride(token)
            .then((response) => {
                setState('success');
                setMessage(response.data.message);
            })
            .catch((error) => {
                setState('error');
                setMessage(error instanceof Error ? error.message : 'Verification failed.');
            });
    }, [verifyEmailOverride]);

    let heading = 'Verifying';
    if (state === 'success') {
        heading = 'Email verified';
    } else if (state === 'error') {
        heading = 'Verification didn’t complete';
    }

    return (
        <main className="flex min-h-screen items-center justify-center bg-canvas px-4 py-12 text-ink">
            <div className="w-full max-w-lg border border-rule bg-surface enter">
                <div className="border-b border-rule px-8 py-6">
                    <p className="caption">Notification email</p>
                    <h1 className="mt-2 text-[22px] font-medium tracking-tight text-ink">{heading}</h1>
                </div>

                <div className="px-8 py-6">
                    {state === 'verifying' && (
                        <p className="flex items-center gap-3 text-[13px] text-ink-soft" aria-live="polite">
                            <span className="relative flex h-2 w-2">
                                <span className="ember absolute inset-0 rounded-full bg-accent opacity-40" aria-hidden="true" />
                                <span className="relative m-auto h-1 w-1 rounded-full bg-accent" />
                            </span>
                            {message}
                        </p>
                    )}

                    {state === 'success' && (
                        <p className="flex items-start gap-3 text-[14px] text-ink">
                            <CheckCircle2 className="mt-0.5 h-4 w-4 flex-shrink-0 text-affirm" aria-hidden="true" />
                            <span>{message}</span>
                        </p>
                    )}

                    {state === 'error' && (
                        <p className="flex items-start gap-3 border border-warn/40 bg-warn-soft px-3 py-2 text-[13px] text-ink" role="alert">
                            <TriangleAlert className="mt-0.5 h-4 w-4 flex-shrink-0 text-warn" aria-hidden="true" />
                            <span>{message}</span>
                        </p>
                    )}

                    <a
                        className="btn-accent mt-6 inline-flex h-10 items-center justify-center rounded-md px-4 text-[13px] font-medium"
                        href="/"
                    >
                        Return to settings
                    </a>
                </div>
            </div>
        </main>
    );
}
