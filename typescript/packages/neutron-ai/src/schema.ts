import { AIError, problemFromStatus } from "./errors.js";
import { zodToJsonSchema } from "./internal/zod-to-json-schema.js";
import type { StandardSchemaV1 } from "./standard-schema.js";

export type JSONSchemaObject = Record<string, unknown>;

export type SchemaValidationResult<T> =
  | { success: true; value: T }
  | { success: false; issues: ReadonlyArray<{ message: string; path?: string }> };

const SCHEMA_BRAND = Symbol.for("neutron-ai.schema");

/**
 * The resolved schema shape everything downstream consumes: JSON Schema for
 * the provider wire, a validate function for inputs coming back from the
 * model. Built via jsonSchema() or resolved from any Standard Schema
 * validator (zod, valibot, arktype, ...).
 */
export interface Schema<T = unknown> {
  readonly [SCHEMA_BRAND]: true;
  readonly jsonSchema: JSONSchemaObject;
  validate(value: unknown): SchemaValidationResult<T> | Promise<SchemaValidationResult<T>>;
}

/** Anything accepted where a schema is expected. */
export type FlexibleSchema<T = unknown> = Schema<T> | StandardSchemaV1<unknown, T>;

export type InferSchema<S> = S extends Schema<infer T>
  ? T
  : S extends StandardSchemaV1<unknown, infer O>
    ? O
    : never;

/**
 * Wrap a raw JSON Schema, optionally with a validator. Without one, values
 * pass through unvalidated — the escape hatch for schema libraries without
 * JSON Schema derivation, or for hand-written schemas.
 */
export function jsonSchema<T = unknown>(
  schema: JSONSchemaObject,
  options: { validate?: (value: unknown) => SchemaValidationResult<T> | Promise<SchemaValidationResult<T>> } = {},
): Schema<T> {
  return {
    [SCHEMA_BRAND]: true,
    jsonSchema: schema,
    validate: options.validate ?? ((value: unknown) => ({ success: true, value: value as T })),
  };
}

export function isSchema<T = unknown>(value: unknown): value is Schema<T> {
  return typeof value === "object" && value !== null && SCHEMA_BRAND in value;
}

function isStandardSchema(value: unknown): value is StandardSchemaV1 {
  return typeof value === "object" && value !== null && "~standard" in value;
}

/** Resolve a flexible schema to the internal Schema shape, once per call site. */
export function resolveSchema<T>(input: FlexibleSchema<T>): Schema<T> {
  if (isSchema<T>(input)) {
    return input;
  }
  if (isStandardSchema(input)) {
    const props = input["~standard"];
    const validate = async (value: unknown): Promise<SchemaValidationResult<T>> => {
      const result = await props.validate(value);
      if (result.issues === undefined) {
        return { success: true, value: result.value as T };
      }
      return {
        success: false,
        issues: result.issues.map((issue) => {
          const path = issue.path
            ?.map((segment) => String(typeof segment === "object" ? segment.key : segment))
            .join(".");
          return path !== undefined && path !== "" ? { message: issue.message, path } : { message: issue.message };
        }),
      };
    };

    if (props.vendor === "zod") {
      return {
        [SCHEMA_BRAND]: true,
        jsonSchema: zodToJsonSchema(input),
        validate,
      };
    }
    throw new AIError(
      problemFromStatus(
        400,
        `Cannot derive JSON Schema from a "${props.vendor}" schema. Wrap it: jsonSchema(theJsonSchema, { validate: yourSchemaValidate }).`,
      ),
    );
  }
  throw new AIError(problemFromStatus(400, "Expected a Standard Schema validator or a jsonSchema() wrapper."));
}
