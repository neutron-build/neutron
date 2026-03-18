import {
  decodeSerializedPayload,
  deserializeTransportData,
} from "../core/serialization.js";
import type { LoaderData } from "./hooks.js";

export function decodeLoaderDataPayload(payload: unknown): LoaderData {
  return decodeSerializedPayload<LoaderData>(payload);
}

export function readInitialLoaderData(): LoaderData {
  if (window.__NEUTRON_DATA__) {
    return window.__NEUTRON_DATA__;
  }

  const serialized = window.__NEUTRON_DATA_SERIALIZED__;
  if (typeof serialized !== "string") {
    if (serialized !== undefined && import.meta.env.DEV) {
      console.warn(
        "[neutron] window.__NEUTRON_DATA_SERIALIZED__ is not a string " +
          `(got ${typeof serialized}). Loader data will be empty. ` +
          "This usually means server serialization failed — check the server console for warnings.",
      );
    }
    return {};
  }

  try {
    const data = deserializeTransportData<LoaderData>(serialized);
    window.__NEUTRON_DATA__ = data;
    return data;
  } catch (err) {
    if (import.meta.env.DEV) {
      console.error(
        "[neutron] Failed to deserialize loader data from __NEUTRON_DATA_SERIALIZED__. " +
          "Components will receive empty data and may render incorrectly.",
        err,
      );
    }
    return {};
  }
}
