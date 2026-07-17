import {
    clearTurnstileVerification,
    hasTurnstileVerification,
    readFreshTurnstileToken,
    requestTurnstileReset,
    storeTurnstileVerification,
    TURNSTILE_RESET_EVENT,
} from '../turnstile';

describe('Turnstile client state', () => {
    beforeEach(() => {
        sessionStorage.clear();
    });

    it('returns only tokens that are still within the provider lifetime', () => {
        storeTurnstileVerification('fresh-token', 10_000);

        expect(readFreshTurnstileToken(10_000 + 4 * 60 * 1000)).toBe('fresh-token');
        expect(readFreshTurnstileToken(10_000 + 4 * 60 * 1000 + 1)).toBeNull();
        expect(hasTurnstileVerification()).toBe(true);
    });

    it('does not trust legacy tokens without a verification marker or timestamp', () => {
        sessionStorage.setItem('jobscout_turnstile_token', 'legacy-token');

        expect(readFreshTurnstileToken()).toBeNull();
        expect(hasTurnstileVerification()).toBe(false);
    });

    it('clears verification and notifies the mounted gate after a backend rejection', () => {
        storeTurnstileVerification('token');
        const listener = vi.fn();
        window.addEventListener(TURNSTILE_RESET_EVENT, listener);

        requestTurnstileReset();

        expect(readFreshTurnstileToken()).toBeNull();
        expect(hasTurnstileVerification()).toBe(false);
        expect(listener).toHaveBeenCalledOnce();
        window.removeEventListener(TURNSTILE_RESET_EVENT, listener);
    });

    it('clears every storage key on logout or account expiry', () => {
        storeTurnstileVerification('token');

        clearTurnstileVerification();

        expect(sessionStorage.length).toBe(0);
    });
});
