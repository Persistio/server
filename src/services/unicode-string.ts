import { z } from 'zod';

/** JSON Schema maxLength counts Unicode code points, not UTF-16 code units. */
export function unicodeStringSchema(maxLength: number) {
  return z.string().superRefine((value, ctx) => {
    const reject = () => ctx.addIssue({ code: z.ZodIssueCode.custom,
      message: `String must contain at most ${maxLength} Unicode code points`, fatal: true });
    // Bound work before iteration or any downstream regex/trim allocation.
    if (value.length > maxLength * 2) return reject();
    let count = 0;
    for (const _character of value) if (++count > maxLength) return reject();
  });
}
