# 04 — Matching & semantic engine

## Goal
Compute an accurate Resume↔JD match score even when the resume is written in different styles (bullets vs sentences), and produce explainable evidence.

## Core concepts
- Resume evidence units: atomic claims (action + context + tech + outcome) extracted from user input.
- JD requirements: normalized requirements grouped into skills, responsibilities, and constraints.
- Matching: hybrid approach (keyword + embedding similarity + structured tags).

## Tasks
### A. Resume ingestion & normalization
- Accept resume input in flexible formats:
- raw text
- structured sections
- imported from PDF/DOCX (later)
- Split into candidate units (sentences/bullets), then normalize into evidence units.
- Store:
- original text
- normalized representation
- embeddings (per unit)
- metadata (section, dates, company)

### B. JD parsing & normalization
- Extract:
- required skills
- preferred skills
- responsibilities
- seniority signals
- location/remote constraints
- compensation signals if present
- Store both raw and normalized forms.

### C. Embedding + vector store
- Choose embedding model(s): local (offline) vs hosted (BYOK).
- Store vectors for resume units and optionally JD requirements.
- Support re-embedding on model change.

### D. Scoring
- Compute a weighted score:
- coverage of required skills
- coverage of responsibilities
- penalties for missing “must-have” constraints
- optional boosts for preferred skills
- Output:
- total score
- top matched requirements
- missing requirements
- evidence mapping (requirement -> one or more resume evidence units)

### E. Explainability rules
- A requirement can be marked “covered” only if:
- there is explicit evidence, OR
- the user manually confirms it.
- Store provenance pointers used later by generation/export gating.

## Acceptance criteria
- Given a JD and a resume, system returns a score + a list of matched/missing items.
- For each matched requirement, UI can show the exact resume text that supports it.

## Risks / gotchas
- Embeddings alone are not enough; you need structured constraints (location, authorization, years).
- Over-aggressive normalization can lose important nuance (e.g., “led” vs “supported”).
