import type OpenAI from 'openai';
import { zodResponseFormat } from 'openai/helpers/zod';
import type { ZodTypeAny } from 'zod';

type Schema = Record<string, unknown>;
const localConstraints = new Set([
  'minimum', 'maximum', 'exclusiveMinimum', 'exclusiveMaximum', 'multipleOf',
  'minLength', 'maxLength', 'minItems', 'maxItems', 'uniqueItems'
]);
const schemaAnnotations = new Set(['$schema', '$id', 'definitions', '$defs']);
const types = new Set(['object', 'array', 'string', 'integer', 'number', 'boolean', 'null']);

function object(value: unknown): value is Schema {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function invalidSchema(): never {
  throw new Error('Unsupported model generation schema');
}

/**
 * The provider schema constrains structure, not our complete business contract.
 * Bounds/refinements remain mandatory in the original Zod validator. This static
 * projection deliberately fails on new structural constructs rather than quietly
 * discarding them. Only application-owned schemas belong here, never source data.
 */
export function modelResponseFormat(schema: ZodTypeAny, name: string): OpenAI.ResponseFormatJSONSchema {
  const generated = zodResponseFormat(schema, name).json_schema.schema;
  if (!object(generated)) invalidSchema();
  const root = generated;

  const resolve = (ref: string): Schema => {
    if (!ref.startsWith('#/')) invalidSchema();
    let value: unknown = root;
    for (const encoded of ref.slice(2).split('/')) {
      const key = encoded.replace(/~1/g, '/').replace(/~0/g, '~');
      if (!object(value) || !Object.hasOwn(value, key)) invalidSchema();
      value = value[key];
    }
    if (!object(value)) invalidSchema();
    return value;
  };

  const project = (value: unknown, refs = new Set<string>()): Schema => {
    if (!object(value)) invalidSchema();
    if (Object.hasOwn(value, '$ref')) {
      const ref = value.$ref;
      if (typeof ref !== 'string' || refs.has(ref)
        || Object.keys(value).some(key => key !== '$ref' && !schemaAnnotations.has(key))) invalidSchema();
      return project(resolve(ref), new Set([...refs, ref]));
    }
    const result: Schema = {};
    let nullable = false;
    for (const [key, item] of Object.entries(value)) {
      if (localConstraints.has(key) || schemaAnnotations.has(key)) continue;
      if (key === 'type') {
        const list = Array.isArray(item) ? item : [item];
        if (list.length === 0 || list.some(type => typeof type !== 'string' || !types.has(type))
          || (list.length > 1 && (list.length !== 2 || !list.includes('null')))) invalidSchema();
        result.type = item;
      } else if (key === 'properties') {
        if (!object(item)) invalidSchema();
        result.properties = Object.fromEntries(Object.entries(item).map(([field, child]) => [field, project(child, refs)]));
      } else if (key === 'items') {
        result.items = project(item, refs);
      } else if (key === 'required') {
        if (!Array.isArray(item) || item.some(field => typeof field !== 'string')) invalidSchema();
        result.required = item;
      } else if (key === 'additionalProperties') {
        if (item !== false) invalidSchema();
        result.additionalProperties = false;
      } else if (key === 'enum' || key === 'const') {
        const values = key === 'const' ? [item] : item;
        if (!Array.isArray(values) || values.length === 0
          || values.some(entry => entry !== null && !['string', 'number', 'boolean'].includes(typeof entry))) invalidSchema();
        result.enum = values;
      } else if (key === 'anyOf') {
        // Nullable unions only. General alternatives need an explicit contract
        // decision instead of a provider-specific expansion of this adapter.
        if (!Array.isArray(item) || item.length !== 2
          || item.filter(child => object(child) && child.type === 'null').length !== 1) invalidSchema();
        result.anyOf = item.map(child => project(child, refs));
      } else if (key === 'nullable') {
        // The public SDK helper emits OpenAPI-style nullable for simple Zod
        // primitives. Translate it to the providers' JSON Schema representation.
        if (typeof item !== 'boolean') invalidSchema();
        nullable = item;
      } else if (['title', 'description', 'pattern', 'format'].includes(key)) {
        if (typeof item !== 'string') invalidSchema();
        result[key] = item;
      } else {
        invalidSchema();
      }
    }
    if (!Object.hasOwn(result, 'type') && !Object.hasOwn(result, 'anyOf')) invalidSchema();
    if (result.type === 'object') {
      if (!object(result.properties) || result.additionalProperties !== false || !Array.isArray(result.required)) invalidSchema();
      const keys = Object.keys(result.properties);
      if (result.required.length !== keys.length || new Set(result.required).size !== keys.length
        || keys.some(key => !(result.required as unknown[]).includes(key))) invalidSchema();
    }
    if (result.type === 'array' && !object(result.items)) invalidSchema();
    if (nullable) {
      if (typeof result.type === 'string') {
        if (result.type !== 'null') result.type = [result.type, 'null'];
      } else if (!Array.isArray(result.type) || !result.type.includes('null')) invalidSchema();
      if (Array.isArray(result.enum) && !result.enum.includes(null)) result.enum = [...result.enum, null];
    }
    return result;
  };

  const projected = project(root);
  if (projected.type !== 'object') invalidSchema();
  return { type: 'json_schema', json_schema: { name, strict: true, schema: projected } };
}
