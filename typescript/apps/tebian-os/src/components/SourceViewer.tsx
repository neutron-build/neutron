import { useState, useEffect } from "preact/hooks";

type FileEntry = {
  name: string;
  path: string;
};

const files: FileEntry[] = [
  { name: "install.sh", path: "install.sh" },
  { name: "tebian-installer", path: "scripts/tebian-installer" },
  { name: "desktop.sh", path: "scripts/desktop.sh" },
  { name: "status.sh", path: "scripts/status.sh" },
];

const REPO = "tebian-os/tebian";
const BRANCH = "main";
const RAW_BASE = `https://raw.githubusercontent.com/${REPO}/${BRANCH}`;
const GITHUB_BASE = `https://github.com/${REPO}/blob/${BRANCH}`;

export function SourceViewer() {
  const [activeFile, setActiveFile] = useState(files[0].path);
  const [contents, setContents] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (contents[activeFile]) return;

    let cancelled = false;
    setLoading(true);
    setError(null);

    fetch(`${RAW_BASE}/${activeFile}`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.text();
      })
      .then((text) => {
        if (cancelled) return;
        setContents((prev) => ({ ...prev, [activeFile]: text }));
        setLoading(false);
      })
      .catch((err) => {
        if (cancelled) return;
        setError(String(err));
        setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [activeFile]);

  const code = contents[activeFile] ?? "";
  const lines = code.split("\n");

  return (
    <div class="editor">
      <div class="tabs">
        {files.map((file) => (
          <button
            key={file.path}
            class={`tab ${file.path === activeFile ? "active" : ""}`}
            onClick={() => setActiveFile(file.path)}
          >
            {file.name}
          </button>
        ))}
        <a
          class="tab tab-github"
          href={`${GITHUB_BASE}/${activeFile}`}
          target="_blank"
          rel="noopener noreferrer"
          title="View on GitHub"
        >
          View on GitHub &rarr;
        </a>
      </div>
      <div class="code-container">
        {loading && <div class="source-status">Loading from GitHub...</div>}
        {error && <div class="source-status source-error">Failed to load: {error}</div>}
        {!loading && !error && code && (
          <>
            <div class="line-numbers">
              {lines.map((_, i) => (
                <span key={i}>{i + 1}</span>
              ))}
            </div>
            <pre class="code">
              <code>{code}</code>
            </pre>
          </>
        )}
      </div>
    </div>
  );
}
