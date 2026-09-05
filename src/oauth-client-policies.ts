import { z } from 'zod';

export const oauthRoutePermissionSchema = z.enum(['plans', 'vaults', 'analytics', 'admin']);
export type OAuthRoutePermission = z.infer<typeof oauthRoutePermissionSchema>;

export const platformOAuthClientPolicySchema = z.object({
  client_id: z.string().min(1),
  allowed_scopes: z.array(z.string().min(1)).min(1),
  allow_delegation: z.boolean().default(false),
  require_account_context: z.boolean().default(false),
  allow_global_access: z.boolean().default(false)
}).strict();

const platformOAuthClientPoliciesSchema = z.array(platformOAuthClientPolicySchema).min(1);

export type PlatformOAuthClientPolicyConfig = z.infer<typeof platformOAuthClientPolicySchema>;

export function parsePlatformOAuthClientPolicies(raw: string): PlatformOAuthClientPolicyConfig[] {
  const trimmed = raw.trim();
  if (!trimmed) {
    throw new Error('PLATFORM_OAUTH_CLIENT_POLICIES must contain at least one OAuth client policy');
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    throw new Error('PLATFORM_OAUTH_CLIENT_POLICIES must be valid JSON');
  }

  const policies = platformOAuthClientPoliciesSchema.parse(parsed);
  const clientIds = new Set<string>();
  for (const policy of policies) {
    if (clientIds.has(policy.client_id)) {
      throw new Error(`Duplicate OAuth client policy for client_id ${policy.client_id}`);
    }
    clientIds.add(policy.client_id);
  }

  return policies;
}
