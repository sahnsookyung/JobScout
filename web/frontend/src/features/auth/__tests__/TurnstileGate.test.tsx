import { act, render, screen } from '@testing-library/react';

import { TurnstileGate } from '../TurnstileGate';
import { requestTurnstileReset, storeTurnstileVerification } from '@/utils/turnstile';

describe('TurnstileGate', () => {
    beforeEach(() => {
        vi.stubEnv('VITE_TURNSTILE_SITE_KEY', 'site-key');
        sessionStorage.clear();
    });

    afterEach(() => {
        vi.unstubAllEnvs();
        sessionStorage.clear();
    });

    it('reopens the security check when the API rejects cached verification', () => {
        storeTurnstileVerification('token');
        render(<TurnstileGate />);
        expect(screen.queryByRole('region', { name: 'Security check' })).toBeNull();

        act(() => requestTurnstileReset());

        expect(screen.getByRole('region', { name: 'Security check' })).toBeInTheDocument();
    });
});
