import React from 'react';
import { clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
    variant?: 'primary' | 'secondary' | 'ghost' | 'quiet';
    size?: 'sm' | 'md' | 'lg';
    isLoading?: boolean;
}

export const Button: React.FC<ButtonProps> = ({
    children,
    variant = 'primary',
    size = 'md',
    isLoading = false,
    className,
    disabled,
    ...props
}) => {
    return (
        <button
            className={twMerge(clsx(
                'inline-flex items-center justify-center gap-2 rounded-md font-medium whitespace-nowrap',
                'transition-[background-color,border-color,color,opacity] duration-200 ease-out',
                'disabled:pointer-events-none disabled:opacity-50',
                {
                    // Primary — the only place terracotta appears on type
                    'bg-accent text-[#FFF] border border-accent hover:bg-accent-hover hover:border-accent-hover':
                        variant === 'primary',
                    // Secondary — quiet confidence, hairline rule
                    'bg-surface text-ink border border-rule hover:border-rule-strong':
                        variant === 'secondary',
                    // Ghost — nothing until hovered
                    'bg-transparent text-ink-soft border border-transparent hover:bg-surface hover:text-ink':
                        variant === 'ghost',
                    // Quiet — inverse of ghost for dark CTAs on light surfaces
                    'bg-ink text-canvas border border-ink hover:bg-ink-soft hover:border-ink-soft':
                        variant === 'quiet',
                    'px-3 py-1.5 text-[13px] leading-tight': size === 'sm',
                    'px-4 py-2 text-[14px]': size === 'md',
                    'px-5 py-2.5 text-[15px]': size === 'lg',
                },
                className,
            ))}
            disabled={disabled || isLoading}
            aria-busy={isLoading}
            {...props}
        >
            {isLoading && (
                <svg
                    className="h-3.5 w-3.5 animate-spin"
                    xmlns="http://www.w3.org/2000/svg"
                    fill="none"
                    viewBox="0 0 24 24"
                    aria-hidden="true"
                >
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="3" />
                    <path
                        className="opacity-75"
                        fill="currentColor"
                        d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                    />
                </svg>
            )}
            {children}
        </button>
    );
};
