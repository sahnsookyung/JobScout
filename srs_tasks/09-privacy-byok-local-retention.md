# 09 — Privacy modes (BYOK, local/offline, retention)

## Goal
Make privacy a first-class differentiator: users control keys, storage, and whether data leaves their machine.

## Tasks
### A. BYOK
- UI to add/update/remove LLM API keys.
- Encrypt keys at rest.
- Allow per-feature routing: embeddings local vs hosted.

### B. Local/offline mode
- Support self-host “all-in-one” docker-compose where:
- database is local
- vector DB is local
- generation runs locally
- Provide a “no external calls” toggle.

### C. Data retention
- Settings:
- keep raw JDs? (yes/no)
- auto-delete raw JDs after X days
- keep embeddings? (yes/no)
- export/delete account
- Job delete should be soft-delete first.

### D. Audit and transparency
- “Where your data went” log for:
- LLM calls (provider, model, timestamp, token count estimate)
- export actions

## Acceptance criteria
- User can run core functionality without sending resume/JD content to external services.
- User can wipe their data and confirm deletion.

## Risks / gotchas
- Local models vary widely in quality; provide clear “quality vs privacy” tradeoffs.
