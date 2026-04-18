import { type ReactNode, type RefObject, useEffect, useRef } from 'react';
import { X } from 'lucide-react';

const FOCUSABLE_SELECTOR = [
    'button:not([disabled])',
    '[href]',
    'input:not([disabled])',
    'select:not([disabled])',
    'textarea:not([disabled])',
    '[tabindex]:not([tabindex="-1"])',
].join(', ');

function useModalFocusTrap(
    ref: RefObject<HTMLElement | null>,
    enabled: boolean,
    onDismiss: () => void,
) {
    const previousFocusRef = useRef<HTMLElement | null>(null);

    useEffect(() => {
        if (!enabled) return;

        const modalElement = ref.current;
        if (!modalElement) return;

        previousFocusRef.current =
            document.activeElement instanceof HTMLElement ? document.activeElement : null;

        const focusableElements = () =>
            Array.from(modalElement.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR)).filter(
                (element) => !element.hasAttribute('disabled') && element.tabIndex !== -1,
            );

        const frameId = requestAnimationFrame(() => {
            const [firstFocusable] = focusableElements();
            (firstFocusable ?? modalElement).focus();
        });

        const handleKeyDown = (event: KeyboardEvent) => {
            if (event.key === 'Escape') {
                event.preventDefault();
                onDismiss();
                return;
            }

            if (event.key !== 'Tab') {
                return;
            }

            const focusables = focusableElements();
            if (focusables.length === 0) {
                event.preventDefault();
                modalElement.focus();
                return;
            }

            const firstFocusable = focusables[0];
            const lastFocusable = focusables[focusables.length - 1];
            const activeElement =
                document.activeElement instanceof HTMLElement ? document.activeElement : null;

            if (!event.shiftKey && activeElement === lastFocusable) {
                event.preventDefault();
                firstFocusable.focus();
            }

            if (event.shiftKey && activeElement === firstFocusable) {
                event.preventDefault();
                lastFocusable.focus();
            }
        };

        document.addEventListener('keydown', handleKeyDown);

        return () => {
            cancelAnimationFrame(frameId);
            document.removeEventListener('keydown', handleKeyDown);
            previousFocusRef.current?.focus();
        };
    }, [enabled, onDismiss, ref]);
}

type ModalShellProps = Readonly<{
    isOpen: boolean;
    onClose: () => void;
    titleId: string;
    eyebrow: string;
    title: string;
    description?: string;
    closeLabel?: string;
    maxWidth?: string;
    children: ReactNode;
}>;

export function ModalShell({
    isOpen,
    onClose,
    titleId,
    eyebrow,
    title,
    description,
    closeLabel = 'Close',
    maxWidth = 'max-w-4xl',
    children,
}: ModalShellProps) {
    const dialogRef = useRef<HTMLDialogElement>(null);

    useModalFocusTrap(dialogRef, isOpen, onClose);

    if (!isOpen) return null;

    return (
        <dialog
            ref={dialogRef}
            open
            aria-labelledby={titleId}
            aria-modal="true"
            tabIndex={-1}
            className="fixed inset-0 z-50 m-0 h-full max-h-none w-full max-w-none overflow-y-auto border-0 bg-transparent p-0 backdrop:bg-[rgba(23,20,15,0.58)]"
        >
            <button
                type="button"
                className="fixed inset-0"
                aria-label={closeLabel}
                tabIndex={-1}
                onClick={onClose}
            />
            <div className="pointer-events-none flex min-h-full items-start justify-center px-4 py-12 sm:py-16">
                <div
                    className={`pointer-events-auto relative w-full ${maxWidth} overflow-hidden rounded-md border border-rule bg-surface enter`}
                >
                    <header className="flex items-start justify-between gap-6 border-b border-rule bg-surface-raised px-7 py-6 sm:px-9">
                        <div>
                            <p className="caption">{eyebrow}</p>
                            <h2 id={titleId} className="mt-2 text-[22px] font-medium tracking-tight text-ink">
                                {title}
                            </h2>
                            {description && (
                                <p className="mt-2 max-w-xl text-[14px] text-ink-soft">{description}</p>
                            )}
                        </div>
                        <button
                            type="button"
                            onClick={onClose}
                            className="rounded-sm p-1 text-ink-muted transition-colors hover:bg-surface-sunk hover:text-ink"
                            aria-label={closeLabel}
                            data-autofocus="true"
                        >
                            <X className="h-4 w-4" />
                        </button>
                    </header>
                    <div className="max-h-[76vh] overflow-y-auto bg-canvas px-7 py-7 sm:px-9">
                        {children}
                    </div>
                </div>
            </div>
        </dialog>
    );
}
