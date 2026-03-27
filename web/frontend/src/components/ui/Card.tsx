import React from 'react';
import { clsx } from 'clsx';

interface CardProps {
    children: React.ReactNode;
    className?: string;
    onClick?: () => void;
}

export const Card: React.FC<CardProps> = ({ children, className, onClick }) => {
    const classes = clsx(
        'bg-white rounded-xl border border-gray-100 shadow-md transition-all duration-300 ease-in-out',
        onClick && 'block w-full appearance-none p-0 text-left font-inherit cursor-pointer hover:shadow-xl hover:-translate-y-1',
        className
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
