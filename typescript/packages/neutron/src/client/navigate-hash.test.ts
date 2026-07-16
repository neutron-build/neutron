// @vitest-environment happy-dom
import { describe, it, expect, beforeEach } from "vitest";
import { navigate } from "./navigate.js";
import type { RouteHref } from "../core/typed-routes.js";

// Regression: the SPA click interceptor and navigate() used to build the target
// from pathname + search only, dropping the hash — so anchor links (#section,
// /page#section) never scrolled. navigate() must preserve the hash in the URL.
describe("navigate hash preservation", () => {
  beforeEach(() => {
    window.history.replaceState(null, "", "/");
  });

  it("keeps the hash when navigating to another page", () => {
    navigate("/about#team" as RouteHref);
    expect(window.location.pathname).toBe("/about");
    expect(window.location.hash).toBe("#team");
  });

  it("keeps the hash when only the hash changes on the same route", () => {
    window.history.replaceState(null, "", "/docs/routing");
    navigate("/docs/routing#configuration" as RouteHref);
    expect(window.location.pathname).toBe("/docs/routing");
    expect(window.location.hash).toBe("#configuration");
  });
});
