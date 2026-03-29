import { type RefObject, useEffect, useMemo, useRef, useState } from 'react';
import {
    Bell,
    Briefcase,
    LogOut,
    UserCircle2,
    X,
} from 'lucide-react';

import { Button } from '@/components/ui/Button';
import { useAuth } from '@/features/auth/useAuth';
import { NotificationSettingsPanel } from '@/features/notifications/components/NotificationSettingsPanel';

function useDismissOnOutsideClick(
    ref: RefObject<HTMLElement | null>,
    enabled: boolean,
    onDismiss: () => void,
) {
    useEffect(() => {
        if (!enabled) {
            return;
        }

        const handlePointerDown = (event: MouseEvent) => {
            if (ref.current && !ref.current.contains(event.target as Node)) {
                onDismiss();
            }
        };

        document.addEventListener('mousedown', handlePointerDown);
        return () => document.removeEventListener('mousedown', handlePointerDown);
    }, [enabled, onDismiss, ref]);
}

function useEscapeDismiss(enabled: boolean, onDismiss: () => void) {
    useEffect(() => {
        if (!enabled) {
            return;
        }

        const handleKeyDown = (event: KeyboardEvent) => {
            if (event.key === 'Escape') {
                onDismiss();
            }
        };

        document.addEventListener('keydown', handleKeyDown);
        return () => document.removeEventListener('keydown', handleKeyDown);
    }, [enabled, onDismiss]);
}

function NotificationSettingsModal({
    isOpen,
    onClose,
}: Readonly<{
    isOpen: boolean;
    onClose: () => void;
}>) {
    useEscapeDismiss(isOpen, onClose);

    if (!isOpen) {
        return null;
    }

    return (
        <dialog
            open
            aria-labelledby="notification-settings-title"
            onCancel={(event) => {
                event.preventDefault();
                onClose();
            }}
            className="fixed inset-0 z-50 m-0 h-full max-h-none w-full max-w-none overflow-y-auto border-0 bg-transparent p-0 backdrop:bg-slate-950/55 backdrop:backdrop-blur-sm"
        >
            <button
                type="button"
                className="fixed inset-0"
                aria-label="Close notification settings"
                onClick={onClose}
            />
            <div className="pointer-events-none flex min-h-full items-center justify-center p-4 sm:p-6">
                <div className="pointer-events-auto relative w-full max-w-4xl overflow-hidden rounded-[2rem] border border-slate-200 bg-white shadow-2xl">
                    <div className="relative overflow-hidden border-b border-slate-200 bg-gradient-to-r from-slate-950 via-blue-950 to-slate-900 px-6 py-6 sm:px-8">
                        <div className="absolute inset-y-0 right-0 w-56 bg-[radial-gradient(circle_at_top_right,_rgba(96,165,250,0.32),_transparent_70%)]" />
                        <div className="relative flex items-start justify-between gap-4">
                            <div>
                                <p className="text-xs font-semibold uppercase tracking-[0.32em] text-sky-200">
                                    Delivery Settings
                                </p>
                                <h2
                                    id="notification-settings-title"
                                    className="mt-2 text-2xl font-black text-white sm:text-3xl"
                                >
                                    Notification preferences
                                </h2>
                                <p className="mt-2 max-w-2xl text-sm text-slate-200">
                                    Decide which alerts matter, where they should land, and when to test them.
                                </p>
                            </div>

                            <button
                                type="button"
                                onClick={onClose}
                                className="rounded-2xl border border-white/20 bg-white/10 p-2 text-white transition hover:bg-white/20"
                                aria-label="Close notification settings"
                            >
                                <X className="h-5 w-5" />
                            </button>
                        </div>
                    </div>

                    <div className="max-h-[78vh] overflow-y-auto bg-gradient-to-b from-slate-50 via-white to-sky-50 px-6 py-6 sm:px-8">
                        <NotificationSettingsPanel />
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
                subtitle: 'Authenticated session',
            };
        }

        return {
            name: 'Local workspace',
            email: 'Frontend auth is not enabled in this environment',
            picture: undefined,
            subtitle: 'Backend dev-bypass identity may still be active',
        };
    }, [user]);

    const avatarInitials = initialsFor(identity.name, identity.email);

    const openNotifications = () => {
        setIsProfileOpen(false);
        setIsNotificationModalOpen(true);
    };

    const toggleProfile = () => {
        setIsNotificationModalOpen(false);
        setIsProfileOpen((current) => !current);
    };

    return (
        <>
            <header className="sticky top-0 z-40 border-b border-slate-200/80 bg-white/90 shadow-sm backdrop-blur-xl">
                <div className="mx-auto flex max-w-[1800px] flex-col gap-4 px-4 py-4 sm:px-6 lg:flex-row lg:items-center lg:justify-between lg:px-8">
                    <div className="flex items-center gap-3">
                        <div className="rounded-2xl bg-gradient-to-br from-blue-600 to-sky-500 p-3 shadow-lg shadow-blue-200/80">
                            <Briefcase className="h-6 w-6 text-white" />
                        </div>
                        <div>
                            <h1 className="text-2xl font-black tracking-tight text-slate-950">
                                JobScout Dashboard
                            </h1>
                            <p className="text-sm font-medium text-slate-500">
                                AI-guided matching with a calmer control surface
                            </p>
                        </div>
                    </div>

                    <div className="flex items-center justify-end gap-3">
                        <button
                            type="button"
                            onClick={openNotifications}
                            className="inline-flex h-11 w-11 items-center justify-center rounded-2xl border border-slate-200 bg-white text-slate-700 shadow-sm transition hover:-translate-y-0.5 hover:border-sky-200 hover:text-sky-600 hover:shadow-md"
                            aria-label="Open notification settings"
                        >
                            <Bell className="h-5 w-5" />
                        </button>

                        <div className="relative" ref={profilePanelRef}>
                            <button
                                type="button"
                                onClick={toggleProfile}
                                className="inline-flex items-center gap-3 rounded-2xl border border-slate-200 bg-white px-3 py-2 text-left shadow-sm transition hover:-translate-y-0.5 hover:border-blue-200 hover:shadow-md"
                                aria-haspopup="menu"
                                aria-expanded={isProfileOpen}
                                aria-label="Open profile menu"
                            >
                                {identity.picture ? (
                                    <img
                                        src={identity.picture}
                                        alt=""
                                        className="h-10 w-10 rounded-xl object-cover"
                                    />
                                ) : (
                                    <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-gradient-to-br from-slate-950 to-blue-700 text-sm font-black text-white">
                                        {avatarInitials}
                                    </div>
                                )}
                                <div className="hidden min-w-0 sm:block">
                                    <div className="truncate text-sm font-bold text-slate-900">
                                        {identity.name}
                                    </div>
                                    <div className="truncate text-xs text-slate-500">
                                        {user ? identity.email : 'Profile details'}
                                    </div>
                                </div>
                            </button>

                            {isProfileOpen && (
                                <div
                                    role="menu"
                                    className="absolute right-0 top-full mt-3 w-[22rem] overflow-hidden rounded-[1.75rem] border border-slate-200 bg-white shadow-2xl"
                                >
                                    <div className="bg-gradient-to-r from-slate-950 via-blue-950 to-slate-900 px-5 py-5 text-white">
                                        <div className="flex items-center gap-3">
                                            {identity.picture ? (
                                                <img
                                                    src={identity.picture}
                                                    alt=""
                                                    className="h-14 w-14 rounded-2xl object-cover ring-2 ring-white/20"
                                                />
                                            ) : (
                                                <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-white/12 text-lg font-black">
                                                    {avatarInitials}
                                                </div>
                                            )}
                                            <div className="min-w-0">
                                                <div className="truncate text-lg font-black">
                                                    {identity.name}
                                                </div>
                                                <div className="truncate text-sm text-slate-200">
                                                    {identity.email}
                                                </div>
                                            </div>
                                        </div>
                                    </div>

                                    <div className="space-y-4 px-5 py-5">
                                        <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3">
                                            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">
                                                Session
                                            </p>
                                            <p className="mt-2 text-sm font-semibold text-slate-900">
                                                {identity.subtitle}
                                            </p>
                                        </div>

                                        {user ? (
                                            <Button
                                                type="button"
                                                variant="secondary"
                                                size="sm"
                                                className="w-full justify-center rounded-2xl"
                                                onClick={() => {
                                                    logout();
                                                    setIsProfileOpen(false);
                                                }}
                                            >
                                                <LogOut className="mr-2 h-4 w-4" />
                                                Sign out
                                            </Button>
                                        ) : (
                                            <div className="flex items-center gap-2 rounded-2xl border border-dashed border-slate-200 px-4 py-3 text-sm text-slate-500">
                                                <UserCircle2 className="h-4 w-4" />
                                                Google sign-in is disabled for this local session.
                                            </div>
                                        )}
                                    </div>
                                </div>
                            )}
                        </div>
                    </div>
                </div>
            </header>

            <NotificationSettingsModal
                isOpen={isNotificationModalOpen}
                onClose={() => setIsNotificationModalOpen(false)}
            />
        </>
    );
}
