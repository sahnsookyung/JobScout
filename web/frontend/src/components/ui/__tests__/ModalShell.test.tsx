import { fireEvent, render, screen, waitFor } from '@testing-library/react';

import { ModalShell } from '../ModalShell';

describe('ModalShell', () => {
    it('does not render when closed', () => {
        render(
            <ModalShell
                isOpen={false}
                onClose={() => undefined}
                titleId="modal-title"
                eyebrow="Meta"
                title="Closed modal"
            >
                <p>Hidden content</p>
            </ModalShell>
        );

        expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    });

    it('cycles focus, dismisses, and restores focus when closed', async () => {
        const onClose = vi.fn();
        const { rerender } = render(
            <>
                <button type="button">Open modal</button>
                <ModalShell
                    isOpen
                    onClose={onClose}
                    titleId="modal-title"
                    eyebrow="Meta"
                    title="Modal title"
                    closeLabel="Close modal"
                >
                    <button type="button">Primary action</button>
                    <button type="button">Secondary action</button>
                </ModalShell>
            </>
        );

        const opener = screen.getByRole('button', { name: 'Open modal' });
        opener.focus();

        const closeButtons = screen.getAllByRole('button', { name: /close modal/i });
        const backdropClose = closeButtons[0];
        const headerClose = closeButtons[1];
        const secondaryAction = screen.getByRole('button', { name: 'Secondary action' });

        await waitFor(() => {
            expect(headerClose).toHaveFocus();
        });

        secondaryAction.focus();
        fireEvent.keyDown(document, { key: 'Tab' });
        expect(headerClose).toHaveFocus();

        fireEvent.keyDown(document, { key: 'Tab', shiftKey: true });
        expect(secondaryAction).toHaveFocus();

        fireEvent.keyDown(document, { key: 'Escape' });
        fireEvent.click(backdropClose);
        expect(onClose).toHaveBeenCalledTimes(2);

        rerender(
            <>
                <button type="button">Open modal</button>
                <ModalShell
                    isOpen={false}
                    onClose={onClose}
                    titleId="modal-title"
                    eyebrow="Meta"
                    title="Modal title"
                    closeLabel="Close modal"
                >
                    <p>Hidden content</p>
                </ModalShell>
            </>
        );

        expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
        expect(opener).toBeInTheDocument();
    });
});
