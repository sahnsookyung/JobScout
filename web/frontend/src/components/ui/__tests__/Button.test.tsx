/**
 * Tests for Button component
 * Covers: web/frontend/src/components/ui/Button.tsx
 */

import { render, screen, fireEvent } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import { Button } from '../Button';

describe('Button', () => {
    it('renders children', () => {
        render(<Button>Click me</Button>);
        expect(screen.getByText('Click me')).toBeInTheDocument();
    });

    it('defaults to primary variant', () => {
        const { container } = render(<Button>Primary</Button>);
        const btn = container.querySelector('button')!;
        expect(btn).toHaveClass('bg-blue-600');
    });

    it('applies secondary variant classes', () => {
        const { container } = render(<Button variant="secondary">Sec</Button>);
        const btn = container.querySelector('button')!;
        expect(btn).toHaveClass('bg-gray-200');
    });

    it('applies ghost variant classes', () => {
        const { container } = render(<Button variant="ghost">Ghost</Button>);
        const btn = container.querySelector('button')!;
        expect(btn).toHaveClass('hover:bg-gray-100');
    });

    it('defaults to md size', () => {
        const { container } = render(<Button>Med</Button>);
        const btn = container.querySelector('button')!;
        expect(btn).toHaveClass('px-4', 'py-2');
    });

    it('applies sm size classes', () => {
        const { container } = render(<Button size="sm">Sm</Button>);
        const btn = container.querySelector('button')!;
        expect(btn).toHaveClass('px-3', 'py-1.5', 'text-sm');
    });

    it('applies lg size classes', () => {
        const { container } = render(<Button size="lg">Lg</Button>);
        const btn = container.querySelector('button')!;
        expect(btn).toHaveClass('px-6', 'py-3', 'text-lg');
    });

    it('shows spinner SVG when isLoading', () => {
        const { container } = render(<Button isLoading>Loading</Button>);
        expect(container.querySelector('svg')).toBeInTheDocument();
    });

    it('does not show spinner when not loading', () => {
        const { container } = render(<Button>Not loading</Button>);
        expect(container.querySelector('svg')).not.toBeInTheDocument();
    });

    it('is disabled when isLoading', () => {
        render(<Button isLoading>Loading</Button>);
        expect(screen.getByRole('button')).toBeDisabled();
    });

    it('is disabled when disabled prop set', () => {
        render(<Button disabled>Disabled</Button>);
        expect(screen.getByRole('button')).toBeDisabled();
    });

    it('sets aria-busy when loading', () => {
        render(<Button isLoading>Loading</Button>);
        expect(screen.getByRole('button')).toHaveAttribute('aria-busy', 'true');
    });

    it('does not set aria-busy when not loading', () => {
        render(<Button>Normal</Button>);
        expect(screen.getByRole('button')).toHaveAttribute('aria-busy', 'false');
    });

    it('sets aria-label to Loading... when isLoading', () => {
        render(<Button isLoading>Loading</Button>);
        expect(screen.getByRole('button')).toHaveAttribute('aria-label', 'Loading...');
    });

    it('does not set aria-label when not loading', () => {
        render(<Button>Normal</Button>);
        expect(screen.getByRole('button')).not.toHaveAttribute('aria-label');
    });

    it('merges custom className', () => {
        const { container } = render(<Button className="custom-class">Btn</Button>);
        expect(container.querySelector('button')).toHaveClass('custom-class');
    });

    it('fires onClick handler', () => {
        const onClick = vi.fn();
        render(<Button onClick={onClick}>Click</Button>);
        fireEvent.click(screen.getByRole('button'));
        expect(onClick).toHaveBeenCalledTimes(1);
    });

    it('does not fire onClick when disabled', () => {
        const onClick = vi.fn();
        const { container } = render(<Button disabled onClick={onClick}>Click</Button>);
        const btn = container.querySelector('button')!;
        btn.click();
        expect(onClick).not.toHaveBeenCalled();
    });

    it('passes through additional HTML button attributes', () => {
        render(<Button type="submit" form="my-form">Submit</Button>);
        const btn = screen.getByRole('button');
        expect(btn).toHaveAttribute('type', 'submit');
        expect(btn).toHaveAttribute('form', 'my-form');
    });
});
