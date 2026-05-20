# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for the Persistio platform.

ADRs capture significant architectural decisions — what was decided, why, alternatives considered, and consequences. Their purpose is to prevent re-litigation and ensure future understanding.

---

## Index

| ADR | Title | Status | Date |
|-----|-------|--------|------|
| [ADR-001](ADR-001-api-layer-separation.md) | API Layer Separation — `api.persistio.ai` (platform) vs `app.persistio.ai` (management) | Accepted | 2026-05-20 |
| [ADR-002](ADR-002-oauth2-microsoft-entra.md) | OAuth 2 / Microsoft Entra Authentication — identity backend, app registrations, and token flows | Accepted | 2026-05-20 |

---

## Format

Each ADR follows this structure:

- **Title** — short, descriptive
- **Status** — Proposed / Accepted / Superseded / Deprecated
- **Date** — date accepted
- **Context** — why the decision was needed
- **Decision** — what was decided
- **Alternatives Considered** — what was evaluated and why it was rejected or deferred
- **Consequences** — what the decision implies going forward

## Adding a New ADR

1. Copy an existing ADR as a template.
2. Name it `ADR-NNN-short-title.md` using the next available number.
3. Add an entry to this README index.
4. Open a PR for review.
