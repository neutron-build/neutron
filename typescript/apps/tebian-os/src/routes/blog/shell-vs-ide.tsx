import { PageShell } from "../../components/PageShell";
import "../../styles/post.css";

export const config = { mode: "static" };

export function head() {
  return { title: "IDE vs Shell — Tebian" };
}

export default function ShellVsIde() {
  return (
    <PageShell>
      <main class="post">
        <header>
          <span class="category">Development Philosophy</span>
          <h1>The IDE is a Framework, the Shell is an Engine</h1>
          <p class="meta">February 20, 2026 &bull; 20 min read</p>
        </header>
        <article class="content">
          <p class="lead">Most modern developers spend their day inside an Integrated Development Environment (IDE). They believe that features like "Auto-complete," "Debugger integration," and "Project management" require a massive, multi-gigabyte software suite. In Tebian, we believe this is a mistake. An IDE is a framework that restricts you; the shell is an engine that empowers you.</p>

          <h2>1. The Chromium Hijack</h2>
          <p>Modern IDEs like VSCode are not native applications. They are Electron apps—instances of the Chromium browser running a set of JavaScript scripts. This means that to edit a text file, you are loading a web browser engine that consumes 1GB+ of RAM. This is Engineering Bloat.</p>

          <p>In Tebian, your editor is <strong>Neovim.</strong> Written in C and configured in Lua, it treated your code as a high-performance data stream decades before Electron existed. It starts in milliseconds. It talks directly to the terminal's GPU buffer. It doesn't "Interpret" your UI; it "Executes" it.</p>

          <h2>2. The Unix Pipeline as a Workflow</h2>
          <p>When you use an IDE, you are limited by the buttons the developer provided. If you want to perform a complex search-and-replace across 1,000 files, you wait for an extension to do it. In the shell, you use <strong>Sed and Ag.</strong> You compose a pipeline that performs the work at the speed of your SSD's I/O. You aren't using a tool; you are building a tool on the fly.</p>

          <h2>Conclusion: Return to the Metal</h2>
          <p>Engineering is about efficiency. By choosing the shell over the IDE, you are reclaiming your CPU cycles and your cognitive bandwidth. One ISO. One menu. One shell. Total mastery.</p>
        </article>
      </main>
    </PageShell>
  );
}
