function stripTrailingSlashes(value: string): string {
    let end = value.length;
    while (end > 0 && value[end - 1] === '/') {
        end -= 1;
    }
    return value.slice(0, end);
}

export function normalizeAppBasePath(
    rawBasePath: string | undefined = import.meta.env.VITE_APP_BASE_PATH
): string {
    const trimmed = rawBasePath?.trim();
    if (!trimmed || trimmed === '/') {
        return '';
    }

    const withLeadingSlash = trimmed.startsWith('/') ? trimmed : `/${trimmed}`;
    return stripTrailingSlashes(withLeadingSlash);
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
