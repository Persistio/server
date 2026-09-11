import { z } from 'zod';

export const provenanceIdentitySchema = (maxLength = 512) => z.string().transform((value, ctx) => {
  const reject = () => {
    ctx.addIssue({ code: z.ZodIssueCode.custom, message: `Invalid provenance identity (maximum ${maxLength} Unicode code points; no controls)` });
    return z.NEVER;
  };
  // A code point occupies at most two UTF-16 units. Reject impossible lengths
  // before iteration, regexes, trimming, or any input-sized allocation.
  if (value.length === 0 || value.length > maxLength * 2) return reject();
  let count = 0;
  for (const character of value) {
    if (++count > maxLength || /[\p{Cc}\p{Cs}]/u.test(character)) return reject();
  }
  const normalized = value.trim();
  return normalized.length ? normalized : reject();
});
