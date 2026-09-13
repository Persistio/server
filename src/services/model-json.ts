/** Normalise only a complete JSON transport fence, never repair model output.
 * Callers still require a completed response and validate their entire schema.
 */
export function parseModelJson(raw: string): unknown {
  const text=raw.trim();
  const fenced=/^```(?:json)?[ \t]*\r?\n([\s\S]*?)\r?\n```$/i.exec(text);
  return JSON.parse(fenced ? fenced[1] : text);
}
