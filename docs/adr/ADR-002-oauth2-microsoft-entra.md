# ADR-002: OAuth 2 / Microsoft Entra Authentication

- **Status:** Accepted
- **Date:** 2026-05-20

---

## Context

Persistio requires a robust, modern authentication and authorisation model covering:

- User-facing access to `app.persistio.ai`
- Service-to-service access from `app.persistio.ai` to `api.persistio.ai`
- Potential future multi-tenant platform access by third-party operators

An identity backend and token strategy must be selected before implementation begins.

---

## Decision

**Microsoft Entra (Azure AD)** is the identity backend for all API security.

### App Registrations

Two Entra App Registrations are created:

1. **`api.persistio.ai` (Resource)** — Exposes OAuth 2.0 scopes (e.g. `vault.read`, `vault.write`). Validates `aud`, `iss`, and scope on every inbound request.
2. **`app.persistio.ai` (Client)** — The management layer, acting as a confidential client for both user-facing and service-to-service flows.

### Authentication Flows

| Flow | Used For | Details |
|---|---|---|
| Authorization Code + PKCE | User-facing access to `app.persistio.ai` | Standard browser/SPA flow via Entra |
| Client Credentials | `app.persistio.ai` → `api.persistio.ai` service calls | App uses a service principal; no user identity required at the platform layer |

**Tenant ID** is the unit of identity at the platform layer. User identity does not need to flow through; tenant ID in the payload is sufficient.

### Deferred Decision: Single-Tenant vs Multi-Tenant App Registration

The question of whether Entra app registrations are single-tenant or multi-tenant is **deferred** — to be resolved before implementation begins.

Multi-tenant is likely the correct choice given the platform-as-a-product direction, but introduces token validation complexity (issuer varies per tenant). This decision must be made deliberately.

---

## Alternatives Considered

### API Keys
Static secrets per tenant or per integration.

**Rejected** — not Modern Auth, carries rotation overhead, and lacks standard revocation semantics. Does not compose well with Entra-based identity.

### On-Behalf-Of (OBO) Flow
Propagate the end-user's identity from `app.persistio.ai` through to `api.persistio.ai`.

**Rejected** — user identity at the platform layer is unnecessary overhead. The platform operates on tenant-scoped resources; tenant ID in the token or payload is sufficient. OBO adds complexity without benefit.

### VNet Isolation (no public API)
Expose `api.persistio.ai` only over a private Virtual Network; no Modern Auth required for internal traffic.

**Deferred/Rejected for now** — the platform API is intended to be fully public-facing with Modern Auth. VNet isolation remains an architectural option for future hardening.

---

## Consequences

- All API requests to `api.persistio.ai` must carry a valid Entra-issued JWT with correct `aud`, `iss`, and scope claims.
- `app.persistio.ai` acquires tokens via Client Credentials for all service-to-service calls to the platform API.
- User sessions at `app.persistio.ai` use Authorization Code + PKCE; user tokens are not forwarded to the platform.
- A managed identity upgrade path is kept open for future Azure infrastructure hosting; the current model does not preclude this.
- Single-tenant vs multi-tenant app registration must be resolved before implementation — see deferred decision above.
- Token validation logic at `api.persistio.ai` must handle the chosen tenant model correctly.
