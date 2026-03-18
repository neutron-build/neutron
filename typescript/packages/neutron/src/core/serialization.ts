import { parse as parseDevalue, stringify as stringifyDevalue } from "devalue";

const SERIALIZED_PAYLOAD_KEY = "__neutron_serialized__";

export interface SerializedPayloadEnvelope {
  [SERIALIZED_PAYLOAD_KEY]: string;
}

export function serializeTransportData(value: unknown): string {
  return stringifyDevalue(value);
}

export type WarnFn = (message: string) => void;

const defaultWarn: WarnFn = (msg) => console.warn(msg);

/**
 * Try to roundtrip a value through devalue. Some types (e.g. Symbol) stringify
 * without throwing but produce output that can't be parsed back.
 */
function tryRoundtrip(value: unknown): string | null {
  try {
    const s = stringifyDevalue(value);
    parseDevalue(s); // verify the output is actually parseable
    return s;
  } catch {
    return null;
  }
}

/**
 * Resilient variant of `serializeTransportData`.
 *
 * Fast path: if `devalue.stringify(value)` roundtrips, return immediately.
 * On failure: walk the top-level record (keyed by route ID), test each route's
 * data individually, then walk per-key inside failing routes. Unserializable
 * keys are stripped and a warning is logged for each one.
 */
export function safeSerializeTransportData(
  value: unknown,
  warn: WarnFn = defaultWarn,
): string {
  // Fast path — most of the time everything is serializable
  const fast = tryRoundtrip(value);
  if (fast !== null) return fast;

  // value should be a record keyed by route ID
  if (!value || typeof value !== "object") {
    warn(
      `[neutron] Cannot serialize loader data (${typeof value}). ` +
        "Returning empty data.",
    );
    return stringifyDevalue({});
  }

  const record = value as Record<string, unknown>;
  const sanitized: Record<string, unknown> = {};

  for (const routeId of Object.keys(record)) {
    const routeData = record[routeId];

    // Try serializing the whole route's data first
    if (tryRoundtrip(routeData) !== null) {
      sanitized[routeId] = routeData;
      continue;
    }

    // If the route data is not a plain object we can't safely walk its keys.
    // Arrays would be corrupted into { "0": ..., "1": ... } by Object.keys
    // reconstruction, so drop the entire route data instead.
    if (
      !routeData ||
      typeof routeData !== "object" ||
      Array.isArray(routeData)
    ) {
      warn(
        `[neutron] Cannot serialize loader data for route "${routeId}" ` +
          `(${Array.isArray(routeData) ? "Array" : typeof routeData}). ` +
          "Dropping entire route data.",
      );
      sanitized[routeId] = {};
      continue;
    }

    const routeRecord = routeData as Record<string, unknown>;
    const cleanedRoute: Record<string, unknown> = {};

    for (const key of Object.keys(routeRecord)) {
      if (tryRoundtrip(routeRecord[key]) !== null) {
        cleanedRoute[key] = routeRecord[key];
      } else {
        warn(
          `[neutron] Cannot serialize key "${key}" in route "${routeId}" ` +
            `(type: ${typeof routeRecord[key]}). Stripping from loader data.`,
        );
      }
    }

    sanitized[routeId] = cleanedRoute;
  }

  // Defensive: the sanitized object should always serialize since every
  // value passed tryRoundtrip, but guard against unexpected edge cases.
  try {
    return stringifyDevalue(sanitized);
  } catch {
    warn(
      "[neutron] Sanitized loader data still failed to serialize. " +
        "Returning empty data.",
    );
    return stringifyDevalue({});
  }
}

export function deserializeTransportData<T = unknown>(serialized: string): T {
  return parseDevalue(serialized) as T;
}

export function encodeSerializedPayload(value: unknown): SerializedPayloadEnvelope {
  return {
    [SERIALIZED_PAYLOAD_KEY]: safeSerializeTransportData(value),
  };
}

export function decodeSerializedPayload<T = unknown>(payload: unknown): T {
  if (!isSerializedPayloadEnvelope(payload)) {
    return payload as T;
  }
  return deserializeTransportData<T>(payload[SERIALIZED_PAYLOAD_KEY]);
}

export function encodeSerializedPayloadAsJson(value: unknown): string {
  return JSON.stringify(encodeSerializedPayload(value));
}

export function serializeForInlineScript(value: unknown): string {
  return escapeJsonForInlineScript(safeSerializeTransportData(value));
}

export function escapeJsonForInlineScript(value: unknown): string {
  return JSON.stringify(value)
    .replace(/</g, "\\u003C")
    .replace(/>/g, "\\u003E")
    .replace(/&/g, "\\u0026")
    .replace(/\u2028/g, "\\u2028")
    .replace(/\u2029/g, "\\u2029");
}

function isSerializedPayloadEnvelope(
  payload: unknown
): payload is SerializedPayloadEnvelope {
  if (!payload || typeof payload !== "object") {
    return false;
  }
  const candidate = payload as Record<string, unknown>;
  return typeof candidate[SERIALIZED_PAYLOAD_KEY] === "string";
}
