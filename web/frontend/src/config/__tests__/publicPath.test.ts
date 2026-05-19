import {
    normalizeAppBasePath,
    stripAppBasePath,
    withAppBasePath,
} from '../publicPath';

describe('public path helpers', () => {
    it('normalizes empty and root base paths to an empty prefix', () => {
        expect(normalizeAppBasePath(undefined)).toBe('');
        expect(normalizeAppBasePath('')).toBe('');
        expect(normalizeAppBasePath('/')).toBe('');
    });

    it('normalizes the JobScout subpath without a trailing slash', () => {
        expect(normalizeAppBasePath('jobscout/')).toBe('/jobscout');
        expect(normalizeAppBasePath('/jobscout/')).toBe('/jobscout');
    });

    it('prefixes API paths when the app is mounted below the domain root', () => {
        expect(withAppBasePath('/api/matches', '/jobscout')).toBe(
            '/jobscout/api/matches'
        );
        expect(withAppBasePath('api/matches', '/jobscout')).toBe(
            '/jobscout/api/matches'
        );
    });

    it('strips the mounted app path for client-side route checks', () => {
        expect(stripAppBasePath('/jobscout/verify-email', '/jobscout')).toBe(
            '/verify-email'
        );
        expect(stripAppBasePath('/jobscout', '/jobscout')).toBe('/');
        expect(stripAppBasePath('/dashboard', '/jobscout')).toBe('/dashboard');
    });
});
