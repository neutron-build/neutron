import { describe, expect, it, vi } from "vitest";
import {
  decodeSerializedPayload,
  deserializeTransportData,
  encodeSerializedPayload,
  encodeSerializedPayloadAsJson,
  safeSerializeTransportData,
  serializeForInlineScript,
  serializeTransportData,
} from "./serialization.js";

describe("serialization", () => {
  it("round-trips non-JSON values through transport serialization", () => {
    const payload = {
      now: new Date("2026-02-12T00:00:00.000Z"),
      set: new Set([1, 2, 3]),
      map: new Map([
        ["one", 1],
        ["two", 2],
      ]),
      big: BigInt("9007199254740993"),
      nan: Number.NaN,
      inf: Number.POSITIVE_INFINITY,
      regex: /hello/gi,
    };

    const serialized = serializeTransportData(payload);
    const restored = deserializeTransportData<typeof payload>(serialized);

    expect(restored.now).toBeInstanceOf(Date);
    expect(restored.now.toISOString()).toBe(payload.now.toISOString());
    expect(restored.set).toBeInstanceOf(Set);
    expect(Array.from(restored.set.values())).toEqual([1, 2, 3]);
    expect(restored.map).toBeInstanceOf(Map);
    expect(restored.map.get("one")).toBe(1);
    expect(restored.big).toBe(payload.big);
    expect(Number.isNaN(restored.nan)).toBe(true);
    expect(restored.inf).toBe(Number.POSITIVE_INFINITY);
    expect(restored.regex).toBeInstanceOf(RegExp);
    expect(restored.regex.source).toBe("hello");
    expect(restored.regex.flags).toContain("g");
    expect(restored.regex.flags).toContain("i");
  });

  it("encodes and decodes envelope payloads", () => {
    const value = {
      date: new Date("2026-02-12T10:00:00.000Z"),
      big: BigInt(5),
    };

    const envelope = encodeSerializedPayload(value);
    const decoded = decodeSerializedPayload<typeof value>(envelope);

    expect(decoded.date).toBeInstanceOf(Date);
    expect(decoded.big).toBe(BigInt(5));
  });

  it("preserves backwards compatibility for plain JSON payloads", () => {
    const payload = { ok: true, count: 2 };
    expect(decodeSerializedPayload(payload)).toEqual(payload);
  });

  it("serializes payload as JSON envelope string", () => {
    const json = encodeSerializedPayloadAsJson({
      date: new Date("2026-02-12T10:00:00.000Z"),
    });
    const parsed = JSON.parse(json) as Record<string, unknown>;
    expect(typeof parsed.__neutron_serialized__).toBe("string");
  });

  it("escapes inline serialization payload safely", () => {
    const output = serializeForInlineScript({
      html: "</script><script>alert(1)</script>",
    });
    expect(output.startsWith("\"")).toBe(true);
    expect(output.includes("</script>")).toBe(false);
  });
});

describe("safeSerializeTransportData", () => {
  it("passes serializable data through unchanged (fast path)", () => {
    const value = {
      "route:home": { title: "Home", count: 42 },
      "route:about": { name: "About" },
    };
    const warn = vi.fn();
    const serialized = safeSerializeTransportData(value, warn);
    const restored = deserializeTransportData<typeof value>(serialized);

    expect(restored).toEqual(value);
    expect(warn).not.toHaveBeenCalled();
  });

  it("strips function values with warning", () => {
    const value = {
      "route:home": {
        title: "Home",
        render: () => "html",
      },
    };
    const warn = vi.fn();
    const serialized = safeSerializeTransportData(value, warn);
    const restored = deserializeTransportData<Record<string, Record<string, unknown>>>(serialized);

    expect(restored["route:home"].title).toBe("Home");
    expect(restored["route:home"].render).toBeUndefined();
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn.mock.calls[0][0]).toContain("render");
    expect(warn.mock.calls[0][0]).toContain("route:home");
  });

  it("strips nested non-serializable values per-key", () => {
    const value = {
      "route:blog": {
        posts: [{ id: 1 }],
        callback: function handler() {},
        sym: Symbol("test"),
      },
    };
    const warn = vi.fn();
    const serialized = safeSerializeTransportData(value, warn);
    const restored = deserializeTransportData<Record<string, Record<string, unknown>>>(serialized);

    expect(restored["route:blog"].posts).toEqual([{ id: 1 }]);
    expect(restored["route:blog"].callback).toBeUndefined();
    expect(restored["route:blog"].sym).toBeUndefined();
    expect(warn).toHaveBeenCalledTimes(2);
  });

  it("drops non-object route data that fails serialization", () => {
    const value = {
      "route:home": { title: "Home" },
      "route:broken": Symbol("bad"),
    };
    const warn = vi.fn();
    const serialized = safeSerializeTransportData(value, warn);
    const restored = deserializeTransportData<Record<string, unknown>>(serialized);

    expect(restored["route:home"]).toEqual({ title: "Home" });
    expect(restored["route:broken"]).toEqual({});
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn.mock.calls[0][0]).toContain("route:broken");
  });

  it("drops array route data that fails serialization without corrupting to object", () => {
    const value = {
      "route:home": { title: "Home" },
      "route:list": [1, 2, Symbol("bad")],
    };
    const warn = vi.fn();
    const serialized = safeSerializeTransportData(value, warn);
    const restored = deserializeTransportData<Record<string, unknown>>(serialized);

    expect(restored["route:home"]).toEqual({ title: "Home" });
    // Should be dropped to {}, NOT corrupted to { "0": 1, "1": 2 }
    expect(restored["route:list"]).toEqual({});
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn.mock.calls[0][0]).toContain("Array");
    expect(warn.mock.calls[0][0]).toContain("route:list");
  });

  it("warning message includes route ID and key name", () => {
    const value = {
      "route:dashboard": {
        config: () => ({}),
      },
    };
    const warn = vi.fn();
    safeSerializeTransportData(value, warn);

    expect(warn).toHaveBeenCalledTimes(1);
    const msg = warn.mock.calls[0][0] as string;
    expect(msg).toContain("route:dashboard");
    expect(msg).toContain("config");
    expect(msg).toContain("function");
  });
});
