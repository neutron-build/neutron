import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "The Rust & C Synthesis — Tebian" };
}

export default function RustCSynthesis() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Development</span>
          <h1>The Rust & C Synthesis</h1>
          <p class="meta">February 20, 2026 &bull; 15 min read</p>
        </header>
        <article class="content">
          <p class="lead">At the heart of Tebian is a fundamental question: How do we build software that is both unbreakable and blindingly fast? The answer lies in the synthesis of C's raw power and Rust's modern safety.</p>

          <h2>The Legacy of C</h2>
          <p>C is the language of the universe. The Linux kernel, the Sway compositor, and the Fuzzel menu system are all written in C. Why? Because C provides a 1:1 mapping to machine instructions. When you write C, you aren't just "programming"; you are choreographing electrons. There is no runtime, no garbage collector, and no overhead. This is why Tebian idles at 16MB—our core isn't a framework; it's a binary.</p>

          <p>However, C has a price: <strong>Memory Responsibility.</strong> In C, the developer is the architect of the RAM. One misplaced pointer, one uninitialized variable, and the system panics. This is the "fragility" that modern OSs try to hide behind layers of Python or JavaScript.</p>

          <h2>The Rust Revolution</h2>
          <p>Rust enters the Tebian ecosystem not as a replacement for C, but as its successor in the "Safety-Critical" layer. Rust offers the same "zero-cost abstractions" as C—meaning it compiles down to the same efficient machine code—but it adds a <strong>Borrow Checker</strong>. This is a C-level fundamental enforced at compile time.</p>

          <p>In Tebian, when we add new tools (like our upcoming <code>t-link</code> fleet manager), we choose Rust. We want the speed of C with the guarantee that a memory leak or a buffer overflow will never crash your session. This is how we achieve "Universal Stability."</p>

          <h2>Why Not Go/Python/Node?</h2>
          <p>Many "modern" distros (like Omakub) rely on tools written in Go or Python. While great for rapid development, they are catastrophic for the "Minimal Base" philosophy. Go binaries are large because they bundle their own runtime. Python requires an interpreter and thousands of supporting files. Node requires a massive C++ engine (V8) just to run a script.</p>

          <p>When you run a Go tool, you are starting a "Mini-OS" inside your OS. When you run a C or Rust binary in Tebian, you are simply asking the CPU to do work. This is the difference between a "Framework Desktop" and a "Binary Desktop."</p>

          <h2>The Fundamental Truth</h2>
          <p>We believe that for an OS to be "Great," it must be built on <strong>Systems Languages.</strong> By sticking to C and Rust, we ensure that every byte of your RAM is serving a purpose. We don't use "Middleware." We use the Metal.</p>
        </article>
      </main>
    </PageShell>
  );
}
