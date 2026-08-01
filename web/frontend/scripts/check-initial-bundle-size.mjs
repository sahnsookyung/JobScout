import { readFile, stat } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const MAX_INITIAL_JAVASCRIPT_BYTES = 500 * 1024;
const projectRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const distRoot = path.join(projectRoot, 'dist');
const html = await readFile(path.join(distRoot, 'index.html'), 'utf8');
const initialAssets = new Set(
    [...html.matchAll(/(?:src|href)="[^"]*\/(assets\/[^"?]+\.js)(?:\?[^\"]*)?"/g)]
        .map((match) => match[1]),
);

if (initialAssets.size === 0) {
    throw new Error('Could not find the initial JavaScript assets in dist/index.html.');
}

let totalBytes = 0;
for (const assetPath of initialAssets) {
    totalBytes += (await stat(path.join(distRoot, assetPath))).size;
}

const totalKib = (totalBytes / 1024).toFixed(1);
const limitKib = MAX_INITIAL_JAVASCRIPT_BYTES / 1024;
console.log(
    `Initial JavaScript: ${totalKib} KiB / ${limitKib} KiB across ${initialAssets.size} files.`,
);

if (totalBytes > MAX_INITIAL_JAVASCRIPT_BYTES) {
    throw new Error(
        `Initial JavaScript exceeds the ${limitKib} KiB performance budget.`,
    );
}
