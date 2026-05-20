# ADR-001: API Layer Separation

- **Status:** Accepted
- **Date:** 2026-05-20

---

## Context

Persistio consists of two distinct concerns:

1. A **platform layer** — vaults, memory, ingest, recall, extract, and jobs. This is the core product and is intended to stand alone as a platform-as-a-product offering.
2. A **management/application layer** — authentication, users, tenants, operators, billing, and subscriptions. This is the operator-facing surface and is responsible for lifecycle management on top of the platform.

An architectural decision was required on whether to deploy these as a monolith or as separate services with a defined API boundary.

---

## Decision

Separate the system into two distinct layers with well-defined API boundaries:

- **`api.persistio.ai`** — Platform layer. Owns: vaults, memory, ingest, recall, extract, jobs. Stands alone. Has no knowledge of users, billing, or tenants beyond a tenant ID.
- **`app.persistio.ai`** — Application/management layer. Owns: User, Tenant, Operator, Plan, and Subscription data models. Manages operator-facing concerns including auth, billing, and subscriptions.

The application layer delegates vault lifecycle operations to the platform API. It stores vault IDs as references and does not manage vault internals directly.

**All inter-layer communication is via APIs only.** There is no direct backend-to-backend database access between layers.

---

## Alternatives Considered

### Monolith
A single service hosting all concerns.

**Rejected** — prevents the platform layer from being offered as a standalone product. Couples operator management concerns to the core memory/vault engine.

### VNet-only inter-layer access
Restrict `api.persistio.ai` to private network access only, with no public exposure, serving only `app.persistio.ai` over a Virtual Network.

**Deferred/Rejected for now** — the platform API is intended to be publicly accessible with Modern Auth. VNet isolation remains an option for future infrastructure hardening but is not required at this stage.

---

## Consequences

- The platform API (`api.persistio.ai`) is independently deployable and usable as a standalone product.
- The application layer (`app.persistio.ai`) must handle all user, tenant, operator, plan, and subscription concerns.
- Any vault operation initiated by the application layer requires an API call to the platform layer — there is no shortcut path.
- API contracts between the layers must be versioned and maintained deliberately.
- Future VNet or private-link isolation is architecturally possible without redesign.
