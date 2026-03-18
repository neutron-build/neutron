import { Island } from "neutron/client";
import Counter from "../components/Counter.js";
import Toggle from "../components/Toggle.js";
import Stopwatch from "../components/Stopwatch.js";

export const config = { mode: "app" };

export default function IslandsDemo() {
  return (
    <div>
      <h1>Islands Demo</h1>
      <p style="margin-bottom: 2rem; color: #888;">
        This page demonstrates islands: interactive components that hydrate progressively.
        The page uses app mode so components are available for hydration.
      </p>

      <h2>client:load</h2>
      <p style="color: #888; margin-bottom: 1rem;">
        Hydrates immediately when the page loads.
      </p>
      <Island component={Counter} client="load" start={0} label="Clicks" />

      <h2 style="margin-top: 2rem;">client:visible</h2>
      <p style="color: #888; margin-bottom: 1rem;">
        Hydrates when scrolled into view. Scroll down to see.
      </p>
      <Island component={Toggle} client="visible" initialOn={false} />
      
      <div style="height: 50vh;" />
      
      <h2 style="margin-top: 2rem;">client:idle</h2>
      <p style="color: #888; margin-bottom: 1rem;">
        Hydrates when the browser is idle.
      </p>
      <Island component={Stopwatch} client="idle" title="Timer" />

      <h2 style="margin-top: 2rem;">Multiple Islands</h2>
      <p style="color: #888; margin-bottom: 1rem;">
        Each island hydrates independently based on its directive.
      </p>
      
      <div style="display: grid; gap: 1rem; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));">
        <Island component={Counter} client="load" start={10} label="A" />
        <Island component={Counter} client="load" start={20} label="B" />
        <Island component={Counter} client="load" start={30} label="C" />
      </div>

      <h2 style="margin-top: 2rem;">How Islands Work</h2>
      <ul>
        <li><strong>SSR:</strong> Component renders to HTML on the server</li>
        <li><strong>Marker:</strong> Wrapped in <code>&lt;neutron-island&gt;</code> with props</li>
        <li><strong>Hydration:</strong> Preact hydrates when directive triggers</li>
        <li><strong>Progressive:</strong> Only hydrate what's needed, when needed</li>
      </ul>
      
      <h2 style="margin-top: 2rem;">Client Directives</h2>
      <table>
        <thead>
          <tr><th>Directive</th><th>Behavior</th></tr>
        </thead>
        <tbody>
          <tr><td><code>client:load</code></td><td>Hydrate immediately</td></tr>
          <tr><td><code>client:visible</code></td><td>Hydrate when in viewport</td></tr>
          <tr><td><code>client:idle</code></td><td>Hydrate on browser idle</td></tr>
          <tr><td><code>client:media="(query)"</code></td><td>Hydrate when media matches</td></tr>
          <tr><td><code>client:only</code></td><td>Client-only, no SSR</td></tr>
        </tbody>
      </table>
    </div>
  );
}
