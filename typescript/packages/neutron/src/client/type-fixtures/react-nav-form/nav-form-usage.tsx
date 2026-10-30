/**
 * Fail-before fixture: navigation/form components used from REACT-typed JSX.
 *
 * A react-compat app typechecks against @types/react
 * (`jsxImportSource: "react"`) while the runtime stays preact/compat. This
 * module is what such an app's route/layout code looks like from that side —
 * React-authored JSX (hooks + elements) hosting <Form>, <Link>, <NavLink>,
 * <ViewTransitions> and <ScrollReveal> with element children, function
 * children and event-handler props, the shapes settings.tsx-style pages use.
 *
 * Compiled only by navform.react-types.test.ts with the tsconfig in this
 * directory; deliberately excluded from the package build (which is
 * jsxImportSource: preact).
 */
import { useState } from "react";
import type { JsxChildren } from "@neutron-build/core/client";
import {
  Form,
  Link,
  NavLink,
  ViewTransitions,
  ScrollReveal,
  Image,
  Picture,
} from "@neutron-build/core/client";
import { Await } from "@neutron-build/core";

/**
 * The scaffolded root layout's shape: a children prop that must accept
 * elements from React-typed JSX AND be renderable from it — preact's
 * ComponentChildren fails the render position here (TS2322, `object` is not
 * a ReactNode), which is what JsxChildren exists for.
 */
export function Layout(props: { children?: JsxChildren }) {
  return (
    <div>
      <nav />
      <main>{props.children}</main>
    </div>
  );
}

export function ReactShell() {
  const [saved, setSaved] = useState(false);

  return (
    <Layout>
      <ViewTransitions />
      <ScrollReveal injectCSS={false} />

      <nav>
        <NavLink to="/dashboard" end activeClass="active" className="nav-item">
          Dashboard
        </NavLink>
        <Link to="/users/42" prefetch="intent" target="_self">
          Users
        </Link>
      </nav>

      <Form
        method="post"
        action="/dashboard"
        style={{ display: "grid", gap: "0.75rem" }}
        onSubmit={(event) => {
          // React-typed event handler prop.
          setSaved(false);
          event.preventDefault();
        }}
      >
        <label>
          Name
          <input type="text" name="name" defaultValue="Acme" />
        </label>
        <button type="submit" disabled={saved}>
          Save
        </button>
      </Form>

      <Form method="get">
        {(state) => (
          <p className={state.submitting ? "busy" : "idle"}>
            {state.error ? state.error.message : "ready"}
          </p>
        )}
      </Form>

      {/* Already JSX-agnostic since their own commits (Image/Picture from the
          initial commit, Await later); pinned here so the react-compat docs
          can state the full client surface's status with evidence. */}
      <Image src="/hero.jpg" alt="Hero" width={640} priority />
      <Picture src="/chart.png" alt="Chart" widths={[320, 640]} formats={["webp"]} />
      <Await resolve={Promise.resolve(42)}>{(value) => <p>v{value}</p>}</Await>
    </Layout>
  );
}
