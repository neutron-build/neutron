// Deliberately bounded generator for this reviewed OpenAPI artifact, not a handler generator.
import { readFileSync, writeFileSync } from 'node:fs';
const spec = JSON.parse(readFileSync(new URL('../../api/openapi.json', import.meta.url), 'utf8'));
function type(schema) {
  if (schema.$ref) return schema.$ref.split('/').at(-1);
  if (schema.anyOf) return schema.anyOf.map(type).join(' | ');
  if (Array.isArray(schema.type)) return schema.type.map(t => type({type:t})).join(' | ');
  if (schema.type === 'object') return '{\n' + Object.entries(schema.properties).map(([key,s]) => `    ${key}${schema.required?.includes(key) ? '' : '?'}: ${type(s)};`).join('\n') + '\n}';
  if (schema.type === 'array') return `Array<${type(schema.items)}>`;
  if (schema.type === 'integer' || schema.type === 'number') return 'number';
  if (['string','boolean','null'].includes(schema.type)) return schema.type;
  throw new Error('Unsupported schema: '+JSON.stringify(schema));
}
const output = '// Generated from ../api/openapi.json; run npm run generate:api. Do not edit.\n' + Object.entries(spec.components.schemas).map(([name,s]) => `export type ${name} = ${type(s)};\n`).join('\n');
const destination = new URL('../src/api-types.ts', import.meta.url);
if (process.argv.includes('--check')) {
  if (readFileSync(destination,'utf8') !== output) throw new Error('API types stale; run npm run generate:api');
} else writeFileSync(destination, output);
