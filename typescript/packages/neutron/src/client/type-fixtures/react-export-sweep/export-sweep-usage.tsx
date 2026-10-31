/**
 * Final-sweep fixture: every component export reachable from React-typed
 * JSX, not already covered by a dedicated fixture family.
 *
 * The JSX-agnosticity campaign covered its members one family at a time —
 * react-island/ (Island), react-nav-form/ (Form, Link, NavLink,
 * ViewTransitions, ScrollReveal), react-control-flow/ (Show, For, Switch,
 * Match, Index) and react-server-island/ (ServerIsland). This module holds
 * the rest of the sweep: <Await> (exported from the package ROOT entry) and
 * <Image>/<Picture> (exported from @neutron-build/core/client), whose
 * JSX-agnostic element returns predate the campaign. Compiling this from
 * `jsxImportSource: "react"` is the proof that no exported component in the
 * public surface is left failing TS2786 — and the @ts-expect-error pins
 * below keep prop checking honest on the react side.
 *
 * Compiled only by exportsweep.react-types.test.ts with the tsconfig in this
 * directory; deliberately excluded from the package build (which is
 * jsxImportSource: preact).
 */
import { useState } from "react";
import { Await } from "@neutron-build/core";
import { Image, Picture } from "@neutron-build/core/client";

/** Await: the keyed children callback with resolved data and an errorElement. */
export function AwaitedStats(props: { stats: Promise<{ views: number }> }) {
  return (
    <Await resolve={props.stats} errorElement={<p>Stats unavailable</p>}>
      {(stats) => <p>{stats.views} views</p>}
    </Await>
  );
}

/** Image: responsive hero with sizes, priority and dimensions. */
export function HeroImage() {
  return (
    <Image
      src="/img/hero.jpg"
      alt="Neutron hero"
      width={1200}
      height={600}
      sizes="100vw"
      priority
    />
  );
}

/** Picture: art-directed sources across formats with a lazy default. */
export function AvatarPicture(props: { src: string }) {
  return (
    <Picture
      src={props.src}
      alt="Avatar"
      widths={[96, 192]}
      formats={["webp", "jpeg"]}
      sizes="96px"
      class="avatar"
    />
  );
}

/** A React-state consumer alongside the framework components, as in a port. */
export function ToggledImage(props: { src: string }) {
  const [show, setShow] = useState(true);
  return (
    <div>
      <button onClick={() => setShow(!show)}>toggle</button>
      {show ? <Image src={props.src} alt="toggleable" /> : null}
    </div>
  );
}

/**
 * Props stay fully typed under this import source too: each pin below must
 * keep erroring (an unused @ts-expect-error fails this fixture's compile).
 */
export function SweepNegatives() {
  return (
    <div>
      {/* @ts-expect-error Await children must be the keyed render callback */}
      <Await resolve={Promise.resolve(1)}>plain</Await>
      {/* @ts-expect-error `alt` is required on Image */}
      <Image src="/img/none.jpg" />
      {/* @ts-expect-error `loading` on Picture must be "lazy" or "eager" */}
      <Picture src="/img/none.jpg" alt="none" loading="soon" />
    </div>
  );
}
