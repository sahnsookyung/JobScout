# 05 — Tailoring, generation, diff, and exports

## Goal
Generate a job-specific resume variant that is evidence-backed, reviewable (diff), and exportable in ATS-friendly formats.

## Recommended architecture
A dedicated “generation service” that receives:
- master resume evidence units
- JD normalized requirements
- constraints (rewrite-only, evidence-required)
- template selection
And returns:
- tailored content
- provenance map
- diff metadata

## Tasks
### A. Tailoring strategy (MVP)
- Reorder + emphasize relevant bullets.
- Rewrite-only mode that rephrases but does not invent new claims.
- Block unsupported requirements from being asserted.

### B. Diff engine
- Store master version and tailored version.
- Compute semantic diff (line-based + section-based):
- moved
- rewritten
- deleted
- added
- UI shows a clear “what changed” view.

### C. Approval gating
- Toggle: “approval required”.
- Export endpoints must enforce:
- if approval_required=true and changes not approved → deny export.
- Store per-change approvals (or per-document approval for MVP).

### D. Export pipeline
- Produce ATS-friendly PDF + DOCX + Markdown.
- Keep a default template; add company-specific templates later.

### E. Integrate Resume-Matcher container (as baseline)
- Run Resume-Matcher as a local service/container when possible.
- Confirm how to submit resume + JD to obtain match score and keyword insights; use it as a secondary signal initially.
- If Resume-Matcher needs code changes for clean API calls, fork and add:
- stable HTTP endpoints
- input validation
- stateless mode

### F. Provenance enforcement
- Every generated bullet must link to one or more evidence units.
- If the generator produces text without evidence linkage, mark it as “needs user evidence” and block “auto-claim”.

## Acceptance criteria
- User can generate a tailored resume and see a diff.
- Export is blocked until approval when approval_required is enabled.
- Exports render consistently with a simple ATS template.

## Risks / gotchas
- Diffing natural language is tricky; keep deterministic formatting before diff.
- DOCX generation is often the most brittle—treat it as a first-class test surface.

## Important considerations
- Also check which country the company is based in and make sure to generate resume improvement criteria that we can use to improve the resume.