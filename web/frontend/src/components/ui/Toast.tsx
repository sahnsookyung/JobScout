import type { CSSProperties } from 'react';
import { Toaster } from 'sonner';

export const ToastProvider = () => (
    <Toaster
        position="bottom-right"
        theme="system"
        visibleToasts={3}
        toastOptions={{
            className: 'sonner-toast',
        }}
        style={{
            '--mobile-offset': '10px',
        } as CSSProperties}
    />
);

export { toast } from 'sonner';
