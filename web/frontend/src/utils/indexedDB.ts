import { RESUME_INDEXEDDB_NAME } from '@shared/constants';

const DB_NAME = RESUME_INDEXEDDB_NAME;
const DB_VERSION = 2;
const STORE_NAME = 'resumes';
const MAX_AGE_MS = 4 * 60 * 60 * 1000;
const MAX_ENTRIES = 1;

interface ResumeEntry {
  key: string;
  owner_id: string;
  file: Blob;
  timestamp: number;
  hash: string;
  filename?: string;
}

let dbPromise: Promise<IDBDatabase> | null = null;
let currentOwnerId: string | null = null;

function ownerId(): string {
  return currentOwnerId ?? 'local';
}

function resumeKey(hash: string, owner = ownerId()): string {
  return `${owner}:${hash}`;
}

export function setResumeOwnerContext(owner: string | null): void {
  currentOwnerId = owner;
}

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
  return Date.now() - entry.timestamp >= MAX_AGE_MS;
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
  return requestToPromise(
    store.get(resumeKey(hash)),
    `Failed to load resume entry for ${hash}`
  );
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
        db.createObjectStore(STORE_NAME, { keyPath: 'key' });
      } else if (event.oldVersion < DB_VERSION) {
        db.deleteObjectStore(STORE_NAME);
        db.createObjectStore(STORE_NAME, { keyPath: 'key' });
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
    key: resumeKey(hash),
    owner_id: ownerId(),
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
    store.getAll(),
    'Failed to load resume entries'
  )) as ResumeEntry[];
  const hash = keys.find((entry) => entry.owner_id === ownerId())?.hash;

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
  await requestToPromise(store.delete(resumeKey(hash)), `Failed to delete resume ${hash}`);
}

export async function deleteOwnerResumes(owner = ownerId()): Promise<void> {
  const db = await openDB();
  const store = getStore(db, 'readwrite');
  const entries = (await requestToPromise(
    store.getAll(),
    'Failed to load owner resume entries'
  )) as ResumeEntry[];
  await deleteHashesFromStore(
    store,
    entries.filter((entry) => entry.owner_id === owner).map((entry) => entry.key)
  );
}

async function cleanupOldEntries(db: IDBDatabase): Promise<void> {
  const store = getStore(db, 'readwrite');
  const entries = (await requestToPromise(
    store.getAll(),
    'Failed to load saved resumes for cleanup'
  )) as ResumeEntry[];
  const hashesToDelete = entries
    .filter((entry) => entry.owner_id === ownerId())
    .map((entry) => ({
      hash: entry.key,
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
