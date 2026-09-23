import type { ComponentChildren } from "preact";

export default function Layout(props: { children?: ComponentChildren }) {
  return (
    <main style="max-width: 720px; margin: 2rem auto; font-family: system-ui, sans-serif;">
      <h1>Neutron example</h1>
      {props.children}
    </main>
  );
}
