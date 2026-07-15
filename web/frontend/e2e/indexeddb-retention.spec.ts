import { expect, test } from '@playwright/test';

const FOUR_HOURS_MS = 4 * 60 * 60 * 1000;
const START_TIME = Date.parse('2026-07-15T00:00:00Z');

test('removes another account resume after the four-hour retention window', async ({
    context,
}) => {
    const firstPage = await context.newPage();
    await firstPage.addInitScript((now) => {
        Date.now = () => now;
    }, START_TIME);
    await firstPage.goto('/favicon.svg');
    await firstPage.evaluate(async () => {
        const storage = await import('/src/utils/indexedDB.ts');
        storage.setResumeOwnerContext('owner-a');
        await storage.saveResume(new Blob(['private resume']), 'owner-a-hash', 'resume.pdf');
    });
    await firstPage.close();

    const secondPage = await context.newPage();
    await secondPage.addInitScript((now) => {
        Date.now = () => now;
    }, START_TIME + FOUR_HOURS_MS + 1);
    await secondPage.goto('/favicon.svg');

    const remainingEntries = await secondPage.evaluate(async () => {
        const storage = await import('/src/utils/indexedDB.ts');
        storage.setResumeOwnerContext('owner-b');
        await storage.hasResume();

        return new Promise<number>((resolve, reject) => {
            const request = indexedDB.open('jobscout-resume', 2);
            request.onerror = () => reject(request.error);
            request.onsuccess = () => {
                const db = request.result;
                const countRequest = db.transaction('resumes', 'readonly')
                    .objectStore('resumes')
                    .count();
                countRequest.onerror = () => reject(countRequest.error);
                countRequest.onsuccess = () => {
                    resolve(countRequest.result);
                    db.close();
                };
            };
        });
    });

    expect(remainingEntries).toBe(0);
});

test('pins the owner when an account changes during an IndexedDB write', async ({ page }) => {
    await page.goto('/favicon.svg');

    const entries = await page.evaluate(async () => {
        const storage = await import('/src/utils/indexedDB.ts');
        storage.setResumeOwnerContext('owner-a');
        const savePromise = storage.saveResume(
            new Blob(['owner a private resume']),
            'same-hash',
            'resume.pdf'
        );
        storage.setResumeOwnerContext('owner-b');
        await savePromise;

        return new Promise<Array<{ key: string; owner_id: string; contents: string }>>(
            (resolve, reject) => {
                const request = indexedDB.open('jobscout-resume', 2);
                request.onerror = () => reject(request.error);
                request.onsuccess = () => {
                    const db = request.result;
                    const getAllRequest = db.transaction('resumes', 'readonly')
                        .objectStore('resumes')
                        .getAll();
                    getAllRequest.onerror = () => reject(getAllRequest.error);
                    getAllRequest.onsuccess = async () => {
                        const storedEntries = getAllRequest.result as Array<{
                            key: string;
                            owner_id: string;
                            file: Blob;
                        }>;
                        resolve(await Promise.all(storedEntries.map(async (entry) => ({
                            key: entry.key,
                            owner_id: entry.owner_id,
                            contents: await entry.file.text(),
                        }))));
                        db.close();
                    };
                };
            }
        );
    });

    expect(entries).toEqual([
        {
            key: 'owner-a:same-hash',
            owner_id: 'owner-a',
            contents: 'owner a private resume',
        },
    ]);
});
