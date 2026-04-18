import React from 'react';
import { clsx } from 'clsx';

interface BadgeProps {
    children: React.ReactNode;
    variant?: 'default' | 'success' | 'error' | 'info' | 'warning' | 'accent';
    className?: string;
}

export const Badge: React.FC<BadgeProps> = ({
    children,
    variant = 'default',
    className,
}) => {
    return (
        <span
            className={clsx(
                'inline-flex items-center gap-1 rounded-sm px-1.5 py-0.5 caption',
                {
                    'bg-surface-sunk text-ink-soft border border-rule': variant === 'default',
                    'bg-affirm-soft text-affirm border border-affirm/40': variant === 'success',
                    // No red anywhere — errors read as clay/ochre
                    'bg-warn-soft text-warn border border-warn/40': variant === 'error' || variant === 'warning',
                    'bg-surface-sunk text-ink border border-rule-strong': variant === 'info',
                    'bg-accent-soft text-accent-ink border border-accent/40': variant === 'accent',
                },
                className,
            )}
        >
            {children}
        </span>
    );
};
