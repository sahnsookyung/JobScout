import { RESUME_INDEXEDDB_NAME, RESUME_MAX_AGE_DAYS } from '@shared/constants';

const DB_NAME = RESUME_INDEXEDDB_NAME;
const DB_VERSION = 1;
const STORE_NAME = 'resumes';
const MAX_AGE_DAYS = RESUME_MAX_AGE_DAYS;
const MAX_ENTRIES = 1;

interface ResumeEntry {
  file: Blob;
  timestamp: number;
  hash: string;
  filename?: string;
}

let dbPromise: Promise<IDBDatabase> | null = null;

function toError(reason: unknown, fallbackMessage: string): Error {
  if (reason instanceof Error) {
    return reason;
  }

  if (typeof reason === 'string' && reason.length > 0) {
    return new Error(reason);
  }

  return new Error(fallbackMessage);
}

function requestToPromise<T>(request: IDBRequest<T>, fallbackMessage: string): Promise<T> {
  return new Promise((resolve, reject) => {
    request.onerror = () => reject(toError(request.error, fallbackMessage));
    request.onsuccess = () => resolve(request.result);
  });
}

function getStore(db: IDBDatabase, mode: IDBTransactionMode): IDBObjectStore {
  return db.transaction(STORE_NAME, mode).objectStore(STORE_NAME);
}

function isExpiredEntry(entry: ResumeEntry): boolean {
  const ageDays = (Date.now() - entry.timestamp) / (1000 * 60 * 60 * 24);
  return ageDays > MAX_AGE_DAYS;
}

function logCleanupFailure(message: string, error: unknown): void {
  console.error(message, toError(error, message));
}

async function deleteExpiredResume(hash: string, message: string): Promise<void> {
  try {
    await deleteResume(hash);
  } catch (error) {
    logCleanupFailure(message, error);
  }
}

async function getResumeEntry(db: IDBDatabase, hash: string): Promise<ResumeEntry | undefined> {
  const store = getStore(db, 'readonly');
  return requestToPromise(store.get(hash), `Failed to load resume entry for ${hash}`);
}

async function getFreshResumeEntry(hash: string): Promise<ResumeEntry | null> {
  const db = await openDB();
  const entry = await getResumeEntry(db, hash);

  if (!entry) {
    return null;
  }

  if (isExpiredEntry(entry)) {
    void deleteExpiredResume(hash, 'Failed to delete expired resume:');
    return null;
  }

  return entry;
}

async function deleteHashesFromStore(store: IDBObjectStore, hashes: string[]): Promise<void> {
  await Promise.all(
    hashes.map((hash) =>
      requestToPromise(store.delete(hash), `Failed to delete ${hash} during cleanup`).catch((error) => {
        logCleanupFailure('Failed to delete entry during cleanup:', error);
      })
    )
  );
}

function openDB(): Promise<IDBDatabase> {
  if (dbPromise) return dbPromise;

  dbPromise = new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);

    request.onerror = () => reject(toError(request.error, 'IndexedDB open failed'));
    request.onsuccess = () => resolve(request.result);
    request.onblocked = () => reject(new Error('IndexedDB open blocked'));

    request.onupgradeneeded = (event) => {
      const db = (event.target as IDBOpenDBRequest).result;
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME, { keyPath: 'hash' });
      }
    };
  });

  dbPromise.catch(() => {
    dbPromise = null;
  });

  return dbPromise;
}

export async function saveResume(file: Blob, hash: string, filename?: string): Promise<void> {
  const db = await openDB();
  const store = getStore(db, 'readwrite');
  const entry: ResumeEntry = {
    file,
    timestamp: Date.now(),
    hash,
    filename,
  };

  await requestToPromise(store.put(entry), `Failed to save resume ${hash}`);
  await cleanupOldEntries(db);
}

export async function getResume(hash: string): Promise<Blob | null> {
  const entry = await getFreshResumeEntry(hash);
  return entry?.file ?? null;
}

export async function getResumeHash(): Promise<string | null> {
  const db = await openDB();
  const store = getStore(db, 'readonly');
  const keys = (await requestToPromise(
    store.getAllKeys(),
    'Failed to load resume keys'
  )) as string[];
  const hash = keys[0];

  if (!hash) {
    return null;
  }

  const entry = await getResumeEntry(db, hash);
  if (!entry) {
    return null;
  }

  if (isExpiredEntry(entry)) {
    void deleteExpiredResume(hash, 'Failed to delete expired resume:');
    return null;
  }

  return hash;
}

export async function getResumeFilename(): Promise<string | null> {
  const hash = await getResumeHash();
  if (!hash) return null;

  const entry = await getFreshResumeEntry(hash);
  return entry?.filename ?? null;
}

export async function deleteResume(hash: string): Promise<void> {
  const db = await openDB();
  const store = getStore(db, 'readwrite');
  await requestToPromise(store.delete(hash), `Failed to delete resume ${hash}`);
}

async function cleanupOldEntries(db: IDBDatabase): Promise<void> {
  const store = getStore(db, 'readwrite');
  const count = await requestToPromise(store.count(), 'Failed to count saved resumes');

  if (count <= MAX_ENTRIES) {
    return;
  }

  const entries = (await requestToPromise(
    store.getAll(),
    'Failed to load saved resumes for cleanup'
  )) as ResumeEntry[];
  const hashesToDelete = entries
    .map((entry) => ({
      hash: entry.hash,
      age: Date.now() - entry.timestamp,
    }))
    .sort((a, b) => a.age - b.age)
    .slice(MAX_ENTRIES)
    .map((entry) => entry.hash);

  if (hashesToDelete.length === 0) {
    return;
  }

  await deleteHashesFromStore(store, hashesToDelete);
}

export async function hasResume(): Promise<boolean> {
  const hash = await getResumeHash();
  return hash !== null;
}
