import React from 'react';
import { clsx } from 'clsx';

interface CardProps {
    children: React.ReactNode;
    className?: string;
    onClick?: () => void;
    variant?: 'default' | 'sunk' | 'raised';
}

export const Card: React.FC<CardProps> = ({ children, className, onClick, variant = 'default' }) => {
    const classes = clsx(
        'border border-rule transition-colors duration-200 ease-out',
        {
            'bg-surface': variant === 'default',
            'bg-surface-sunk': variant === 'sunk',
            'bg-surface-raised': variant === 'raised',
        },
        onClick && 'block w-full appearance-none p-0 text-left font-inherit cursor-pointer hover:border-rule-strong',
        className,
    );

    if (onClick) {
        return (
            <button type="button" className={classes} onClick={onClick}>
                {children}
            </button>
        );
    }

    return <div className={classes}>{children}</div>;
};
