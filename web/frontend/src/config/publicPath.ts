export function normalizeAppBasePath(
    rawBasePath: string | undefined = import.meta.env.VITE_APP_BASE_PATH
): string {
    const trimmed = rawBasePath?.trim();
    if (!trimmed || trimmed === '/') {
        return '';
    }

    const withLeadingSlash = trimmed.startsWith('/') ? trimmed : `/${trimmed}`;
    return withLeadingSlash.replace(/\/+$/u, '');
}

export function withAppBasePath(
    path: string,
    basePath: string = normalizeAppBasePath()
): string {
    const normalizedPath = path.startsWith('/') ? path : `/${path}`;
    return `${basePath}${normalizedPath}`;
}

export function stripAppBasePath(
    pathname: string,
    basePath: string = normalizeAppBasePath()
): string {
    if (!basePath || pathname === basePath) {
        return pathname === basePath ? '/' : pathname;
    }

    if (pathname.startsWith(`${basePath}/`)) {
        return pathname.slice(basePath.length) || '/';
    }

    return pathname;
}
