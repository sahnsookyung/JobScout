import React from 'react';
import { clsx } from 'clsx';

interface BadgeProps {
    children: React.ReactNode;
    variant?: 'default' | 'success' | 'error' | 'info';
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
                'inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium',
                {
                    'bg-gray-100 text-gray-700': variant === 'default',
                    'bg-blue-100 text-blue-800': variant === 'success',
                    'bg-red-50 text-red-700': variant === 'error',
                    'bg-blue-50 text-blue-700': variant === 'info',
                },
                className
            )}
        >
            {children}
        </span>
    );
};
