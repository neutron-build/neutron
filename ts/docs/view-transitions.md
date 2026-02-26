# View Transitions

> **Terminology note:** This page documents **Neutron TypeScript**. In broader ecosystem docs, **Neutron** refers to the umbrella framework/platform across implementations.


Use the `ViewTransitions` component to enable browser-native transitions.

```tsx
import { ViewTransitions } from "neutron";

export default function Layout(props: { children?: unknown }) {
  return (
    <div>
      <ViewTransitions />
      {props.children}
    </div>
  );
}
```

Behavior:

- Static pages: same-origin link clicks are intercepted and animated with `document.startViewTransition()`.
- App routes: router updates use view transitions when enabled.
- Browsers without View Transitions API fall back to normal navigation.
