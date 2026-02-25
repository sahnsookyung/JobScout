import { RESUME_INDEXEDDB_NAME, RESUME_MAX_AGE_DAYS } from './constants';

const DB_NAME = RESUME_INDEXEDDB_NAME;
const DB_VERSION = 1;
const STORE_NAME = 'resumes';
const MAX_AGE_DAYS = RESUME_MAX_AGE_DAYS;
const MAX_ENTRIES = 1;

interface ResumeEntry {
  file: Blob;
  timestamp: number;
  hash: string;
}

function openDB(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);

    request.onerror = () => reject(request.error);
    request.onsuccess = () => resolve(request.result);

    request.onupgradeneeded = (event) => {
      const db = (event.target as IDBOpenDBRequest).result;
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME, { keyPath: 'hash' });
      }
    };
  });
}

export async function saveResume(file: Blob, hash: string): Promise<void> {
  const db = await openDB();

  return new Promise((resolve, reject) => {
    const transaction = db.transaction(STORE_NAME, 'readwrite');
    const store = transaction.objectStore(STORE_NAME);

    const entry: ResumeEntry = {
      file,
      timestamp: Date.now(),
      hash,
    };

    const request = store.put(entry);

    request.onerror = () => reject(request.error);
    request.onsuccess = () => {
      cleanupOldEntries(db).then(resolve).catch(reject);
    };
  });
}

export async function getResume(hash: string): Promise<Blob | null> {
  const db = await openDB();

  return new Promise((resolve, reject) => {
    const transaction = db.transaction(STORE_NAME, 'readonly');
    const store = transaction.objectStore(STORE_NAME);
    const request = store.get(hash);

    request.onerror = () => reject(request.error);
    request.onsuccess = () => {
      const entry = request.result as ResumeEntry | undefined;

      if (!entry) {
        resolve(null);
        return;
      }

      const now = Date.now();
      const ageDays = (now - entry.timestamp) / (1000 * 60 * 60 * 24);

      if (ageDays > MAX_AGE_DAYS) {
        deleteResume(hash).catch(err => console.error('Failed to delete expired resume:', err));
        resolve(null);
        return;
      }

      resolve(entry.file);
    };
  });
}

export async function getResumeHash(): Promise<string | null> {
  const db = await openDB();

  return new Promise((resolve, reject) => {
    const transaction = db.transaction(STORE_NAME, 'readonly');
    const store = transaction.objectStore(STORE_NAME);
    const request = store.getAllKeys();

    request.onerror = () => reject(request.error);
    request.onsuccess = () => {
      const keys = request.result as string[];
      if (keys.length === 0) {
        resolve(null);
        return;
      }

      const hash = keys[0];
      const now = Date.now();

      const getRequest = store.get(hash);
      getRequest.onsuccess = () => {
        const entry = getRequest.result as ResumeEntry | undefined;
        if (!entry) {
          resolve(null);
          return;
        }

        const ageDays = (now - entry.timestamp) / (1000 * 60 * 60 * 24);
        if (ageDays > MAX_AGE_DAYS) {
          deleteResume(hash).catch(err => console.error('Failed to delete expired resume:', err));
          resolve(null);
          return;
        }

        resolve(hash);
      };
    };
  });
}

export async function deleteResume(hash: string): Promise<void> {
  const db = await openDB();

  return new Promise((resolve, reject) => {
    const transaction = db.transaction(STORE_NAME, 'readwrite');
    const store = transaction.objectStore(STORE_NAME);
    const request = store.delete(hash);

    request.onerror = () => reject(request.error);
    request.onsuccess = () => resolve();
  });
}

async function cleanupOldEntries(db: IDBDatabase): Promise<void> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(STORE_NAME, 'readwrite');
    const store = transaction.objectStore(STORE_NAME);
    const countRequest = store.count();

    countRequest.onsuccess = () => {
      if (countRequest.result <= MAX_ENTRIES) {
        resolve();
        return;
      }

      const getAllRequest = store.getAll();
      getAllRequest.onsuccess = () => {
        const entries = getAllRequest.result as ResumeEntry[];
        const now = Date.now();

        const sortedEntries = entries
          .map((entry) => ({
            entry,
            age: now - entry.timestamp,
          }))
          .sort((a, b) => a.age - b.age);

        const toDelete = sortedEntries.slice(0, sortedEntries.length - MAX_ENTRIES);

        let deletedCount = 0;
        toDelete.forEach(({ entry }) => {
          const deleteReq = store.delete(entry.hash);
          deleteReq.onsuccess = () => {
            deletedCount++;
            if (deletedCount === toDelete.length) {
              resolve();
            }
          };
          deleteReq.onerror = () => {
            deletedCount++;
            if (deletedCount === toDelete.length) {
              resolve();
            }
          };
        });
      };
    };

    countRequest.onerror = () => reject(countRequest.error);
  });
}

export async function hasResume(): Promise<boolean> {
  const hash = await getResumeHash();
  return hash !== null;
}
