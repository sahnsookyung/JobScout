import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export default defineConfig({
    plugins: [react()],
    resolve: {
        alias: {
            '@': path.resolve(__dirname, './src'),
            '@shared': path.resolve(__dirname, '../shared'),
        },
    },
    server: {
        port: 5173,
        proxy: {
            '/api': {
                target: 'http://localhost:8080',
                changeOrigin: true,
            },
        },
    },
    test: {
        globals: true,
        environment: 'jsdom',
        setupFiles: ['./src/test/setup.ts'],
        include: ['src/**/*.{test,spec}.{ts,tsx}'],
        // Exclude tests that require browser environment or need Jest-to-Vitest conversion
        exclude: [
            'src/test/**',
            'src/**/*.d.ts',
            'src/main.tsx',
            'src/App.tsx',
            'src/features/dashboard/components/__tests__/**',
            // indexedDB tests require actual browser IndexedDB implementation
            'src/utils/__tests__/indexedDB.test.ts',
        ],
        testTimeout: 10000,
        coverage: {
            provider: 'v8',
            reporter: ['text', 'json', 'html', 'cobertura', 'lcov'],
            reportsDirectory: './coverage',
            include: ['src/**/*.{ts,tsx}'],
            exclude: [
                'src/test/**',
                'src/**/*.d.ts',
                'src/main.tsx',
                'src/App.tsx',
                'src/features/dashboard/components/__tests__/**',
                // indexedDB tests require actual browser IndexedDB implementation
                'src/utils/__tests__/indexedDB.test.ts',
            ],
        },
    },
});
