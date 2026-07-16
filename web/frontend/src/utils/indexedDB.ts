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

async function deleteExpiredResume(
  hash: string,
  message: string,
  owner: string
): Promise<void> {
  try {
    await deleteResumeForOwner(hash, owner);
  } catch (error) {
    logCleanupFailure(message, error);
  }
}

async function getResumeEntry(
  db: IDBDatabase,
  hash: string,
  owner: string
): Promise<ResumeEntry | undefined> {
  const store = getStore(db, 'readonly');
  return requestToPromise(
    store.get(resumeKey(hash, owner)),
    `Failed to load resume entry for ${hash}`
  );
}

async function getFreshResumeEntry(hash: string, owner: string): Promise<ResumeEntry | null> {
  const db = await openDB(owner);
  const entry = await getResumeEntry(db, hash, owner);

  if (!entry) {
    return null;
  }

  if (isExpiredEntry(entry)) {
    void deleteExpiredResume(hash, 'Failed to delete expired resume:', owner);
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

function openDB(cleanupOwner = ownerId()): Promise<IDBDatabase> {
  if (!dbPromise) {
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
  }

  return dbPromise.then(async (db) => {
    await cleanupOldEntries(db, cleanupOwner);
    return db;
  });
}

export async function saveResume(file: Blob, hash: string, filename?: string): Promise<void> {
  const owner = ownerId();
  await deleteOwnerResumes(owner);
  const db = await openDB(owner);
  const store = getStore(db, 'readwrite');
  const entry: ResumeEntry = {
    key: resumeKey(hash, owner),
    owner_id: owner,
    file,
    timestamp: Date.now(),
    hash,
    filename,
  };

  await requestToPromise(store.put(entry), `Failed to save resume ${hash}`);
  await cleanupOldEntries(db, owner);
}

export async function getResume(hash: string): Promise<Blob | null> {
  const owner = ownerId();
  const entry = await getFreshResumeEntry(hash, owner);
  return entry?.file ?? null;
}

async function getResumeHashForOwner(owner: string): Promise<string | null> {
  const db = await openDB(owner);
  const store = getStore(db, 'readonly');
  const keys = (await requestToPromise(
    store.getAll(),
    'Failed to load resume entries'
  )) as ResumeEntry[];
  const hash = keys.find((entry) => entry.owner_id === owner)?.hash;

  if (!hash) {
    return null;
  }

  const entry = await getResumeEntry(db, hash, owner);
  if (!entry) {
    return null;
  }

  if (isExpiredEntry(entry)) {
    void deleteExpiredResume(hash, 'Failed to delete expired resume:', owner);
    return null;
  }

  return hash;
}

export async function getResumeHash(): Promise<string | null> {
  return getResumeHashForOwner(ownerId());
}

export async function getResumeFilename(): Promise<string | null> {
  const owner = ownerId();
  const hash = await getResumeHashForOwner(owner);
  if (!hash) return null;

  const entry = await getFreshResumeEntry(hash, owner);
  return entry?.filename ?? null;
}

async function deleteResumeForOwner(hash: string, owner: string): Promise<void> {
  const db = await openDB(owner);
  const store = getStore(db, 'readwrite');
  await requestToPromise(
    store.delete(resumeKey(hash, owner)),
    `Failed to delete resume ${hash}`
  );
}

export async function deleteResume(hash: string): Promise<void> {
  const owner = ownerId();
  await deleteResumeForOwner(hash, owner);
}

export async function deleteOwnerResumes(owner = ownerId()): Promise<void> {
  const db = await openDB(owner);
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

async function cleanupOldEntries(db: IDBDatabase, owner: string): Promise<void> {
  const store = getStore(db, 'readwrite');
  const entries = (await requestToPromise(
    store.getAll(),
    'Failed to load saved resumes for cleanup'
  )) as ResumeEntry[];
  const expiredKeys = entries
    .filter(isExpiredEntry)
    .map((entry) => entry.key);
  const ownerKeysOverLimit = entries
    .filter((entry) => entry.owner_id === owner && !isExpiredEntry(entry))
    .map((entry) => ({
      key: entry.key,
      age: Date.now() - entry.timestamp,
    }))
    .sort((a, b) => a.age - b.age)
    .slice(MAX_ENTRIES)
    .map((entry) => entry.key);
  const keysToDelete = [...new Set([...expiredKeys, ...ownerKeysOverLimit])];

  if (keysToDelete.length === 0) {
    return;
  }

  await deleteHashesFromStore(store, keysToDelete);
}

export async function hasResume(): Promise<boolean> {
  const hash = await getResumeHashForOwner(ownerId());
  return hash !== null;
}
