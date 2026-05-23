import { defineConfig, type Plugin } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function stripTrailingSlashes(value: string): string {
    let end = value.length;
    while (end > 0 && value[end - 1] === '/') {
        end -= 1;
    }
    return value.slice(0, end);
}

function normalizeBasePath(rawBasePath: string | undefined): string {
    const trimmed = rawBasePath?.trim();
    if (!trimmed || trimmed === '/') {
        return '/';
    }

    const withLeadingSlash = trimmed.startsWith('/') ? trimmed : `/${trimmed}`;
    return `${stripTrailingSlashes(withLeadingSlash)}/`;
}

function cloudflareRocketLoaderOptOut(): Plugin {
    return {
        name: 'cloudflare-rocket-loader-opt-out',
        transformIndexHtml: {
            order: 'post',
            handler(html: string): string {
                return html.replace(
                    /<script\b([^>]*\btype=["']module["'][^>]*)>/g,
                    (scriptTag: string): string => {
                        if (scriptTag.includes('data-cfasync=') || !scriptTag.includes('src=')) {
                            return scriptTag;
                        }

                        return scriptTag.replace('<script', '<script data-cfasync="false"');
                    },
                );
            },
        },
    };
}

export default defineConfig({
    plugins: [react(), cloudflareRocketLoaderOptOut()],
    base: normalizeBasePath(process.env.VITE_APP_BASE_PATH),
    build: {
        cssMinify: false,
    },
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
});
