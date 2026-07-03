import { AIError, problemFromStatus } from "../errors.js";

/**
 * Structural zod-to-JSON-Schema conversion for the subset that tool inputs
 * and structured outputs actually use. Works on both zod internals layouts
 * (v3 `_def`, v4 `_zod.def`) without importing zod, keeping this package
 * dependency-free. Anything outside the subset throws with the jsonSchema()
 * escape hatch named, rather than silently emitting a wrong schema.
 */

type AnyZod = Record<string, any>;

export function zodToJsonSchema(schema: unknown): Record<string, unknown> {
  return convert(schema as AnyZod);
}

function convert(schema: AnyZod): Record<string, unknown> {
  const node = schema?._zod !== undefined ? fromV4(schema) : fromV3(schema);
  const description = typeof schema?.description === "string" ? schema.description : undefined;
  if (description !== undefined && node.description === undefined) {
    node.description = description;
  }
  return node;
}

function unsupported(kind: string): never {
  throw new AIError(
    problemFromStatus(
      400,
      `Unsupported zod construct for JSON Schema conversion: ${kind}. Use a simpler schema or wrap it with jsonSchema().`,
    ),
  );
}

// ---- zod v4 (`schema._zod.def`) ----

function fromV4(schema: AnyZod): Record<string, unknown> {
  const def = schema._zod.def;
  switch (def.type) {
    case "string":
      return { type: "string" };
    case "number": {
      const format = typeof def.format === "string" ? def.format : "";
      return { type: format.includes("int") ? "integer" : "number" };
    }
    case "int":
      return { type: "integer" };
    case "boolean":
      return { type: "boolean" };
    case "null":
      return { type: "null" };
    case "any":
    case "unknown":
      return {};
    case "object": {
      const properties: Record<string, unknown> = {};
      const required: string[] = [];
      for (const [key, value] of Object.entries(def.shape as Record<string, AnyZod>)) {
        properties[key] = convert(value);
        if (!isV4Optionalish(value)) required.push(key);
      }
      const result: Record<string, unknown> = { type: "object", properties, additionalProperties: false };
      if (required.length > 0) result.required = required;
      return result;
    }
    case "array":
      return { type: "array", items: convert(def.element) };
    case "enum":
      return { enum: Object.values(def.entries as Record<string, unknown>) };
    case "literal": {
      const values = def.values as unknown[];
      return values.length === 1 ? { const: values[0] } : { enum: values };
    }
    case "union":
      return { anyOf: (def.options as AnyZod[]).map(convert) };
    case "optional":
    case "nonoptional":
    case "readonly":
      return convert(def.innerType);
    case "nullable":
      return { anyOf: [convert(def.innerType), { type: "null" }] };
    case "default": {
      const inner = convert(def.innerType);
      inner.default = typeof def.defaultValue === "function" ? def.defaultValue() : def.defaultValue;
      return inner;
    }
    case "record":
      return { type: "object", additionalProperties: convert(def.valueType) };
    case "pipe":
      return convert(def.in);
    default:
      unsupported(`v4:${String(def.type)}`);
  }
}

function isV4Optionalish(schema: AnyZod): boolean {
  const type = schema?._zod?.def?.type;
  if (type === "optional" || type === "default") return true;
  if (type === "readonly" || type === "pipe") {
    return isV4Optionalish(schema._zod.def.innerType ?? schema._zod.def.in);
  }
  return false;
}

// ---- zod v3 (`schema._def.typeName`) ----

function fromV3(schema: AnyZod): Record<string, unknown> {
  const def = schema?._def;
  if (def === undefined) unsupported("not a zod schema");
  switch (def.typeName) {
    case "ZodString":
      return { type: "string" };
    case "ZodNumber": {
      const isInt = Array.isArray(def.checks) && def.checks.some((check: AnyZod) => check?.kind === "int");
      return { type: isInt ? "integer" : "number" };
    }
    case "ZodBoolean":
      return { type: "boolean" };
    case "ZodNull":
      return { type: "null" };
    case "ZodAny":
    case "ZodUnknown":
      return {};
    case "ZodObject": {
      const shape = typeof def.shape === "function" ? def.shape() : def.shape;
      const properties: Record<string, unknown> = {};
      const required: string[] = [];
      for (const [key, value] of Object.entries(shape as Record<string, AnyZod>)) {
        properties[key] = convert(value);
        if (!isV3Optionalish(value)) required.push(key);
      }
      const result: Record<string, unknown> = { type: "object", properties, additionalProperties: false };
      if (required.length > 0) result.required = required;
      return result;
    }
    case "ZodArray":
      return { type: "array", items: convert(def.type) };
    case "ZodEnum":
      return { enum: [...(def.values as unknown[])] };
    case "ZodLiteral":
      return { const: def.value };
    case "ZodUnion":
      return { anyOf: (def.options as AnyZod[]).map(convert) };
    case "ZodOptional":
    case "ZodReadonly":
      return convert(def.innerType);
    case "ZodNullable":
      return { anyOf: [convert(def.innerType), { type: "null" }] };
    case "ZodDefault": {
      const inner = convert(def.innerType);
      inner.default = def.defaultValue();
      return inner;
    }
    case "ZodRecord":
      return { type: "object", additionalProperties: convert(def.valueType) };
    case "ZodEffects":
      return convert(def.schema);
    default:
      unsupported(`v3:${String(def.typeName)}`);
  }
}

function isV3Optionalish(schema: AnyZod): boolean {
  const typeName = schema?._def?.typeName;
  if (typeName === "ZodOptional" || typeName === "ZodDefault") return true;
  if (typeName === "ZodReadonly" || typeName === "ZodEffects") {
    return isV3Optionalish(schema._def.innerType ?? schema._def.schema);
  }
  return false;
}
