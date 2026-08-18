import { describe, expect, it } from "vitest";
import { z } from "zod";
import {
  ProblemError,
  badRequest,
  conflict,
  forbidden,
  internalError,
  isProblemError,
  notFoundError,
  rateLimited,
  unauthorized,
  validateJsonBody,
  validationError,
} from "./problem.js";

// The §2 taxonomy: every constructor must carry exactly its contract row.
describe("problem taxonomy constructors", () => {
  const cases: Array<[ProblemError, number, string, string]> = [
    [badRequest("d"), 400, "bad-request", "Bad Request"],
    [unauthorized("d"), 401, "unauthorized", "Unauthorized"],
    [forbidden("d"), 403, "forbidden", "Forbidden"],
    [notFoundError("d"), 404, "not-found", "Not Found"],
    [conflict("d"), 409, "conflict", "Conflict"],
    [validationError("d"), 422, "validation", "Validation Failed"],
    [rateLimited("d"), 429, "rate-limited", "Rate Limited"],
    [internalError("d"), 500, "internal", "Internal Server Error"],
  ];

  it.each(cases)(
    "%o maps to status/code/title",
    (error, status, code, title) => {
      expect(error.status).toBe(status);
      expect(error.code).toBe(code);
      expect(error.title).toBe(title);
      expect(error.detail).toBe("d");
      expect(isProblemError(error)).toBe(true);
    }
  );
});

describe("ProblemError.toResponse", () => {
  it("serves the RFC 7807 required fields as application/problem+json", async () => {
    const response = notFoundError("no such item").toResponse("/api/items/42");
    expect(response.status).toBe(404);
    expect(response.headers.get("content-type")).toBe("application/problem+json");
    expect(await response.json()).toEqual({
      type: "https://neutron.dev/errors/not-found",
      title: "Not Found",
      status: 404,
      detail: "no such item",
      instance: "/api/items/42",
    });
  });

  it("omits instance when not given and includes errors[] when present", async () => {
    const plain = badRequest("bad").toResponse();
    const body = (await plain.json()) as Record<string, unknown>;
    expect("instance" in body).toBe(false);

    const withFields = validationError("Request body failed validation", [
      { field: "name", message: "is required" },
    ]).toResponse();
    const validated = (await withFields.json()) as { errors: unknown[] };
    expect(validated.errors).toEqual([{ field: "name", message: "is required" }]);
  });
});

describe("validateJsonBody", () => {
  const schema = z.object({
    name: z.string().min(1),
    price: z.number().gte(0),
  });

  function jsonRequest(body: string): Request {
    return new Request("http://localhost/api/items", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body,
    });
  }

  it("returns parsed data on success", async () => {
    const parsed = await validateJsonBody(jsonRequest('{"name":"x","price":1}'), schema);
    expect(parsed).toEqual({ name: "x", price: 1 });
  });

  it("throws a 422 validation problem with field/message/value entries", async () => {
    await expect(
      validateJsonBody(jsonRequest('{"name":"","price":-1}'), schema)
    ).rejects.toSatisfy((error: unknown) => {
      if (!isProblemError(error)) return false;
      expect(error.status).toBe(422);
      expect(error.code).toBe("validation");
      expect(error.title).toBe("Validation Failed");
      // Exact zod wording varies by zod version; the contract requires one
      // entry per failing field with field/message, value echoed when primitive.
      expect(error.fields).toEqual([
        { field: "name", message: expect.any(String), value: "" },
        { field: "price", message: expect.any(String), value: -1 },
      ]);
      return true;
    });
  });

  it("throws a 400 bad-request problem for a body that is not JSON", async () => {
    await expect(validateJsonBody(jsonRequest("not json"), schema)).rejects.toSatisfy(
      (error: unknown) => {
        if (!isProblemError(error)) return false;
        expect(error.status).toBe(400);
        expect(error.code).toBe("bad-request");
        return true;
      }
    );
  });
});
