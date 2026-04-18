import { type RefObject, useEffect, useMemo, useRef, useState } from 'react';
import { Bell, LogOut, SlidersHorizontal, UserCircle2, X } from 'lucide-react';

import { Button } from '@/components/ui/Button';
import { ThemeToggle } from '@/components/ui/ThemeToggle';
import { useAuth } from '@/features/auth/useAuth';
import { CandidatePreferencesPanel } from '@/features/preferences/components/CandidatePreferencesPanel';
import { NotificationSettingsPanel } from '@/features/notifications/components/NotificationSettingsPanel';

const FOCUSABLE_SELECTOR = [
    'button:not([disabled])',
    '[href]',
    'input:not([disabled])',
    'select:not([disabled])',
    'textarea:not([disabled])',
    '[tabindex]:not([tabindex="-1"])',
].join(', ');

function useDismissOnOutsideClick(
    ref: RefObject<HTMLElement | null>,
    enabled: boolean,
    onDismiss: () => void,
) {
    useEffect(() => {
        if (!enabled) return;
        const handler = (event: MouseEvent) => {
            if (ref.current && !ref.current.contains(event.target as Node)) {
                onDismiss();
            }
        };
        document.addEventListener('mousedown', handler);
        return () => document.removeEventListener('mousedown', handler);
    }, [enabled, onDismiss, ref]);
}

function useEscapeDismiss(enabled: boolean, onDismiss: () => void) {
    useEffect(() => {
        if (!enabled) return;
        const handler = (event: KeyboardEvent) => {
            if (event.key === 'Escape') onDismiss();
        };
        document.addEventListener('keydown', handler);
        return () => document.removeEventListener('keydown', handler);
    }, [enabled, onDismiss]);
}

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

function ModalShell({
    isOpen,
    onClose,
    titleId,
    eyebrow,
    title,
    description,
    closeLabel = 'Close',
    maxWidth = 'max-w-4xl',
    children,
}: Readonly<{
    isOpen: boolean;
    onClose: () => void;
    titleId: string;
    eyebrow: string;
    title: string;
    description?: string;
    closeLabel?: string;
    maxWidth?: string;
    children: React.ReactNode;
}>) {
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

function initialsFor(name: string, email: string) {
    const source = name.trim() || email.trim();
    const letters = source
        .split(/\s+/)
        .map((part) => part[0] ?? '')
        .join('')
        .slice(0, 2)
        .toUpperCase();
    return letters || 'JS';
}

export function DashboardHeader() {
    const { user, logout } = useAuth();
    const [isNotificationModalOpen, setIsNotificationModalOpen] = useState(false);
    const [isPreferencesModalOpen, setIsPreferencesModalOpen] = useState(false);
    const [isProfileOpen, setIsProfileOpen] = useState(false);
    const profilePanelRef = useRef<HTMLDivElement>(null);

    useDismissOnOutsideClick(profilePanelRef, isProfileOpen, () => setIsProfileOpen(false));
    useEscapeDismiss(isProfileOpen, () => setIsProfileOpen(false));

    const identity = useMemo(() => {
        if (user) {
            return {
                name: user.name,
                email: user.email,
                picture: user.picture,
                subtitle: 'Signed in',
            };
        }
        return {
            name: 'Workshop',
            email: 'Local session',
            picture: undefined,
            subtitle: 'No account signed in',
        };
    }, [user]);

    const avatarInitials = initialsFor(identity.name, identity.email);

    const openNotifications = () => {
        setIsProfileOpen(false);
        setIsPreferencesModalOpen(false);
        setIsNotificationModalOpen(true);
    };
    const openPreferences = () => {
        setIsProfileOpen(false);
        setIsNotificationModalOpen(false);
        setIsPreferencesModalOpen(true);
    };
    const toggleProfile = () => {
        setIsNotificationModalOpen(false);
        setIsPreferencesModalOpen(false);
        setIsProfileOpen((current) => !current);
    };

    return (
        <>
            <header className="sticky top-0 z-40 border-b border-rule bg-canvas/85 backdrop-blur-md">
                <div className="mx-auto flex max-w-[var(--container-content)] items-center justify-between gap-6 px-5 py-4 sm:px-8 lg:px-10">
                    <a href="/" className="group flex items-center gap-3" aria-label="JobScout home">
                        <span className="jobscout-mark" aria-hidden="true" />
                        <span className="flex items-baseline gap-2">
                            <span className="text-[17px] font-medium tracking-tight text-ink">
                                JobScout
                            </span>
                            <span className="caption hidden sm:inline">Workshop</span>
                        </span>
                    </a>

                    <div className="flex items-center gap-2">
                        <ThemeToggle />
                        <button
                            type="button"
                            onClick={openPreferences}
                            className="inline-flex h-9 items-center gap-2 rounded-md border border-rule bg-surface px-3 text-[13px] text-ink-soft transition-colors hover:border-rule-strong hover:text-ink"
                            aria-label="Preferences"
                        >
                            <SlidersHorizontal className="h-4 w-4" aria-hidden="true" />
                            <span className="hidden sm:inline">Preferences</span>
                        </button>
                        <button
                            type="button"
                            onClick={openNotifications}
                            className="inline-flex h-9 items-center gap-2 rounded-md border border-rule bg-surface px-3 text-[13px] text-ink-soft transition-colors hover:border-rule-strong hover:text-ink"
                            aria-label="Open notification settings"
                        >
                            <Bell className="h-4 w-4" aria-hidden="true" />
                            <span className="hidden sm:inline">Notifications</span>
                        </button>

                        <div className="relative" ref={profilePanelRef}>
                            <button
                                type="button"
                                onClick={toggleProfile}
                                className="inline-flex h-9 items-center gap-2 rounded-md border border-rule bg-surface pl-1.5 pr-3 text-left transition-colors hover:border-rule-strong"
                                aria-expanded={isProfileOpen}
                                aria-controls="profile-panel"
                                aria-label="Open profile menu"
                            >
                                {identity.picture ? (
                                    <img
                                        src={identity.picture}
                                        alt=""
                                        className="h-6 w-6 rounded-sm object-cover"
                                    />
                                ) : (
                                    <span className="flex h-6 w-6 items-center justify-center rounded-sm bg-ink text-[10px] font-medium text-canvas">
                                        {avatarInitials}
                                    </span>
                                )}
                                <span className="hidden min-w-0 sm:block">
                                    <span className="block truncate text-[13px] font-medium text-ink">
                                        {identity.name}
                                    </span>
                                </span>
                            </button>

                            {isProfileOpen && (
                                <div
                                    id="profile-panel"
                                    aria-label="Profile panel"
                                    className="absolute right-0 top-full mt-2 w-[20rem] overflow-hidden rounded-md border border-rule bg-surface-raised shadow-lg enter-fade"
                                >
                                    <div className="border-b border-rule px-5 py-4">
                                        <div className="flex items-center gap-3">
                                            {identity.picture ? (
                                                <img
                                                    src={identity.picture}
                                                    alt=""
                                                    className="h-10 w-10 rounded-sm object-cover"
                                                />
                                            ) : (
                                                <span className="flex h-10 w-10 items-center justify-center rounded-sm bg-ink text-[13px] font-medium text-canvas">
                                                    {avatarInitials}
                                                </span>
                                            )}
                                            <div className="min-w-0">
                                                <div className="truncate text-[14px] font-medium text-ink">
                                                    {identity.name}
                                                </div>
                                                <div className="truncate text-[13px] text-ink-muted">
                                                    {identity.email}
                                                </div>
                                            </div>
                                        </div>
                                    </div>

                                    <div className="space-y-3 px-5 py-4">
                                        <div className="flex items-center justify-between gap-2">
                                            <p className="caption">Session</p>
                                            <p className="text-[13px] text-ink-soft">{identity.subtitle}</p>
                                        </div>
                                        {user ? (
                                            <Button
                                                type="button"
                                                variant="secondary"
                                                size="sm"
                                                className="w-full justify-center"
                                                onClick={() => {
                                                    logout();
                                                    setIsProfileOpen(false);
                                                }}
                                            >
                                                <LogOut className="h-3.5 w-3.5" aria-hidden="true" />
                                                Sign out
                                            </Button>
                                        ) : (
                                            <div className="flex items-center gap-2 rounded-sm border border-dashed border-rule px-3 py-2.5 text-[13px] text-ink-muted">
                                                <UserCircle2 className="h-4 w-4" aria-hidden="true" />
                                                Google sign-in is off in this session.
                                            </div>
                                        )}
                                    </div>
                                </div>
                            )}
                        </div>
                    </div>
                </div>
            </header>

            <ModalShell
                isOpen={isNotificationModalOpen}
                onClose={() => setIsNotificationModalOpen(false)}
                titleId="notification-settings-title"
                eyebrow="Delivery"
                title="Notification preferences"
                description="Decide which alerts matter and where they should land."
                closeLabel="Close notification settings"
                maxWidth="max-w-4xl"
            >
                <NotificationSettingsPanel />
            </ModalShell>

            <ModalShell
                isOpen={isPreferencesModalOpen}
                onClose={() => setIsPreferencesModalOpen(false)}
                titleId="candidate-preferences-title"
                eyebrow="Inputs"
                title="Candidate preferences"
                description="Your hard constraints and what matters in your next role."
                closeLabel="Close candidate preferences"
                maxWidth="max-w-5xl"
            >
                <CandidatePreferencesPanel />
            </ModalShell>
        </>
    );
}
