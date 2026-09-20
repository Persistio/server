import { z } from 'zod';
import { unicodeStringSchema } from './unicode-string';

export const provenanceIdentitySchema = (maxLength = 512) => unicodeStringSchema(maxLength).transform((value, ctx) => {
  const reject = () => {
    ctx.addIssue({ code: z.ZodIssueCode.custom, message: `Invalid provenance identity (maximum ${maxLength} Unicode code points; no controls)` });
    return z.NEVER;
  };
  if (/[\p{Cc}\p{Cs}]/u.test(value)) return reject();
  const normalized = value.trim();
  return normalized.length ? normalized : reject();
});
