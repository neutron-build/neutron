import "../styles/manifesto.css";

export const config = { mode: "static" };

export function head() {
  return { title: "Manifesto" };
}

export default function Manifesto() {
  return (
    <>
      <main>
        <article class="manifesto">
          <h1>The Tebian Manifesto</h1>
          <p class="lead">Sovereignty through minimalism. Power through simplicity.</p>

          <section>
            <h2>I. The Law of Zero</h2>
            <p>Performance is not a feature. It is a prerequisite.</p>
            <p>If a process isn't serving the user's immediate goal, it doesn't exist.</p>
            <p><strong>Zero bloat. Zero telemetry. Zero friction.</strong></p>
          </section>

          <section>
            <h2>II. One Question</h2>
            <p>The user should never face choice paralysis.</p>
            <p>Twenty questions become one: <em>Desktop? Y/n</em></p>
            <p>Server or Desktop. Everything else is noise.</p>
          </section>

          <section>
            <h2>III. Honest Foundation</h2>
            <p>Tebian is not a distro. It is a script.</p>
            <p>We do not fork. We do not rebrand. We do not pretend.</p>
            <p>Debian does the heavy lifting. We provide the interface.</p>
          </section>

          <section>
            <h2>IV. The User is Root</h2>
            <p>Every config is visible. Every script is readable.</p>
            <p>The user owns their system. The system serves the user.</p>
            <p>If you can't explain why something exists, it shouldn't.</p>
          </section>

          <section>
            <h2>V. Fractal Sovereignty</h2>
            <p>One config across all nodes.</p>
            <p>Desktop. Server. Raspberry Pi. Phone.</p>
            <p>The Fleet is one brain, many bodies.</p>
          </section>

          <section>
            <h2>VI. Survival of the Fittest</h2>
            <p>We do not kill innovation. We kill redundancy.</p>
            <p>Ten distros survive. Two hundred die.</p>
            <p>Everything else is just noise.</p>
          </section>

          <section class="closing">
            <p>This is not a product.</p>
            <p>This is not a company.</p>
            <p>This is one folder. One script. One choice.</p>
            <p><strong>Debian. Your way.</strong></p>
          </section>
        </article>
      </main>
    </>
  );
}
