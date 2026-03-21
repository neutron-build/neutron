export function generateErrorOverlayScript(): string {
  return `
(function() {
  const STYLES = \`
    :host {
      position: fixed;
      top: 0;
      left: 0;
      width: 100vw;
      height: 100vh;
      z-index: 2147483646;
      font-family: 'Inter', 'SF Pro Display', -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
      pointer-events: none;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }

    .backdrop {
      position: absolute;
      top: 0; left: 0; right: 0; bottom: 0;
      background: rgba(0, 0, 0, 0.6);
      backdrop-filter: blur(4px);
      -webkit-backdrop-filter: blur(4px);
      opacity: 0;
      transition: opacity 0.2s ease;
      pointer-events: auto;
    }
    :host(.visible) .backdrop { opacity: 1; }

    .modal {
      position: absolute;
      top: 50%;
      left: 50%;
      transform: translate(-50%, -50%) scale(0.96);
      width: 680px;
      max-width: calc(100vw - 40px);
      max-height: calc(100vh - 80px);
      background: #0f172a;
      border: 1px solid rgba(129, 140, 248, 0.15);
      border-radius: 16px;
      overflow: hidden;
      display: flex;
      flex-direction: column;
      opacity: 0;
      transition: opacity 0.2s ease, transform 0.2s cubic-bezier(0.16, 1, 0.3, 1);
      pointer-events: auto;
      box-shadow: 0 25px 60px rgba(0, 0, 0, 0.5), 0 0 1px rgba(129, 140, 248, 0.2);
    }
    :host(.visible) .modal {
      opacity: 1;
      transform: translate(-50%, -50%) scale(1);
    }

    .header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 16px 20px;
      border-bottom: 1px solid rgba(129, 140, 248, 0.08);
      flex-shrink: 0;
    }
    .header-left {
      display: flex;
      align-items: center;
      gap: 10px;
    }

    .type-badge {
      font-size: 10px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      padding: 3px 8px;
      border-radius: 6px;
      background: rgba(248, 113, 113, 0.12);
      color: #fca5a5;
    }
    .type-badge.loader { background: rgba(251, 191, 36, 0.12); color: #fbbf24; }
    .type-badge.action { background: rgba(129, 140, 248, 0.12); color: #818cf8; }
    .type-badge.render { background: rgba(248, 113, 113, 0.12); color: #fca5a5; }
    .type-badge.middleware { background: rgba(168, 85, 247, 0.12); color: #c084fc; }
    .type-badge.hydration { background: rgba(56, 189, 248, 0.12); color: #38bdf8; }
    .type-badge.unknown { background: rgba(100, 116, 139, 0.12); color: #94a3b8; }

    .header-actions {
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .btn {
      background: rgba(30, 41, 59, 0.8);
      border: 1px solid rgba(129, 140, 248, 0.1);
      color: #94a3b8;
      cursor: pointer;
      padding: 5px 10px;
      border-radius: 8px;
      font-size: 11px;
      font-weight: 500;
      font-family: inherit;
      transition: all 0.15s;
      display: flex;
      align-items: center;
      gap: 4px;
    }
    .btn:hover { color: #e2e8f0; background: rgba(30, 41, 59, 1); border-color: rgba(129, 140, 248, 0.2); }
    .btn-close {
      width: 28px;
      height: 28px;
      padding: 0;
      justify-content: center;
      border-radius: 8px;
    }

    .body {
      padding: 20px;
      overflow-y: auto;
      flex: 1;
    }
    .body::-webkit-scrollbar { width: 5px; }
    .body::-webkit-scrollbar-track { background: transparent; }
    .body::-webkit-scrollbar-thumb { background: rgba(129, 140, 248, 0.15); border-radius: 3px; }

    .error-message {
      font-size: 16px;
      font-weight: 600;
      color: #fca5a5;
      line-height: 1.5;
      margin-bottom: 12px;
      word-break: break-word;
    }

    .hint {
      font-size: 13px;
      color: #64748b;
      line-height: 1.5;
      margin-bottom: 16px;
      padding: 10px 14px;
      background: rgba(30, 41, 59, 0.5);
      border-radius: 8px;
      border-left: 3px solid #818cf8;
    }

    .file-location {
      font-family: ui-monospace, 'Cascadia Code', 'Source Code Pro', Menlo, Consolas, monospace;
      font-size: 12px;
      color: #818cf8;
      margin-bottom: 12px;
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .file-location span {
      color: #475569;
    }

    .code-frame {
      background: #1e293b;
      border-radius: 10px;
      overflow: hidden;
      margin-bottom: 16px;
      border: 1px solid rgba(129, 140, 248, 0.06);
    }
    .code-line {
      display: flex;
      padding: 0 14px;
      font-family: ui-monospace, 'Cascadia Code', 'Source Code Pro', Menlo, Consolas, monospace;
      font-size: 12px;
      line-height: 22px;
    }
    .code-line.highlight {
      background: rgba(248, 113, 113, 0.1);
      border-left: 3px solid #f87171;
      padding-left: 11px;
    }
    .line-number {
      width: 45px;
      text-align: right;
      color: #334155;
      padding-right: 14px;
      user-select: none;
      flex-shrink: 0;
    }
    .code-line.highlight .line-number { color: #f87171; }
    .line-text {
      color: #94a3b8;
      white-space: pre;
      overflow-x: auto;
    }
    .code-line.highlight .line-text { color: #e2e8f0; }

    .stack-section {
      margin-top: 4px;
    }
    .stack-toggle {
      font-size: 11px;
      color: #475569;
      cursor: pointer;
      user-select: none;
      transition: color 0.12s;
      background: none;
      border: none;
      font-family: inherit;
      padding: 4px 0;
    }
    .stack-toggle:hover { color: #94a3b8; }
    .stack-content {
      display: none;
      margin-top: 8px;
    }
    .stack-content.open { display: block; }
    .stack-pre {
      font-size: 11px;
      color: #475569;
      white-space: pre-wrap;
      word-break: break-all;
      line-height: 1.6;
      padding: 12px 14px;
      background: rgba(15, 23, 42, 0.6);
      border-radius: 8px;
      border: 1px solid rgba(30, 41, 59, 0.6);
      font-family: ui-monospace, 'Cascadia Code', 'Source Code Pro', Menlo, Consolas, monospace;
      max-height: 200px;
      overflow-y: auto;
    }
    .stack-pre::-webkit-scrollbar { width: 4px; }
    .stack-pre::-webkit-scrollbar-track { background: transparent; }
    .stack-pre::-webkit-scrollbar-thumb { background: rgba(100, 116, 139, 0.2); border-radius: 2px; }

    .copied {
      color: #4ade80 !important;
    }
  \`;

  class NeutronErrorOverlay extends HTMLElement {
    constructor() {
      super();
      this.attachShadow({ mode: 'open' });
      this._payload = null;
    }

    connectedCallback() {
      this._buildShell();
    }

    _esc(str) {
      return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    _buildShell() {
      const s = this.shadowRoot;
      s.innerHTML = '';

      const style = document.createElement('style');
      style.textContent = STYLES;
      s.appendChild(style);

      this._backdrop = document.createElement('div');
      this._backdrop.className = 'backdrop';
      this._backdrop.addEventListener('click', () => this.dismiss());
      s.appendChild(this._backdrop);

      this._modal = document.createElement('div');
      this._modal.className = 'modal';
      s.appendChild(this._modal);

      document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') this.dismiss();
      });
    }

    show(payload) {
      this._payload = payload;
      this._renderContent();
      this.classList.add('visible');
    }

    dismiss() {
      this.classList.remove('visible');
      this._payload = null;
    }

    _renderContent() {
      const p = this._payload;
      if (!p) return;

      let html = '<div class="header"><div class="header-left">';
      html += '<span class="type-badge ' + this._esc(p.type) + '">' + this._esc(p.type) + '</span>';
      html += '</div><div class="header-actions">';
      html += '<button class="btn btn-copy" id="copy-btn">Copy Error</button>';
      html += '<button class="btn btn-close" id="close-btn" title="Close"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button>';
      html += '</div></div>';

      html += '<div class="body">';
      html += '<div class="error-message">' + this._esc(p.message) + '</div>';

      if (p.hint) {
        html += '<div class="hint">' + this._esc(p.hint) + '</div>';
      }

      if (p.fileRelative || p.file) {
        const filePath = p.fileRelative || p.file;
        let loc = this._esc(filePath);
        if (p.line != null) {
          loc += '<span>:</span>' + p.line;
          if (p.column != null) {
            loc += '<span>:</span>' + p.column;
          }
        }
        html += '<div class="file-location">' + loc + '</div>';
      }

      if (p.codeFrame && p.codeFrame.lines && p.codeFrame.lines.length > 0) {
        html += '<div class="code-frame">';
        for (const line of p.codeFrame.lines) {
          const cls = line.highlight ? ' highlight' : '';
          html += '<div class="code-line' + cls + '"><span class="line-number">' + line.number + '</span><span class="line-text">' + this._esc(line.text) + '</span></div>';
        }
        html += '</div>';
      }

      if (p.stack) {
        html += '<div class="stack-section">';
        html += '<button class="stack-toggle" id="stack-toggle">Show stack trace</button>';
        html += '<div class="stack-content" id="stack-content"><pre class="stack-pre">' + this._esc(p.stack) + '</pre></div>';
        html += '</div>';
      }

      html += '</div>';

      this._modal.innerHTML = html;

      const closeBtn = this._modal.querySelector('#close-btn');
      if (closeBtn) closeBtn.addEventListener('click', () => this.dismiss());

      const copyBtn = this._modal.querySelector('#copy-btn');
      if (copyBtn) {
        copyBtn.addEventListener('click', () => {
          let text = p.type.toUpperCase() + ': ' + p.message;
          if (p.file) text += '\\n' + p.file + (p.line ? ':' + p.line : '') + (p.column ? ':' + p.column : '');
          if (p.stack) text += '\\n\\n' + p.stack;
          navigator.clipboard.writeText(text).then(() => {
            copyBtn.textContent = 'Copied';
            copyBtn.classList.add('copied');
            setTimeout(() => {
              copyBtn.textContent = 'Copy Error';
              copyBtn.classList.remove('copied');
            }, 2000);
          }).catch(() => {});
        });
      }

      const stackToggle = this._modal.querySelector('#stack-toggle');
      const stackContent = this._modal.querySelector('#stack-content');
      if (stackToggle && stackContent) {
        stackToggle.addEventListener('click', () => {
          const isOpen = stackContent.classList.toggle('open');
          stackToggle.textContent = isOpen ? 'Hide stack trace' : 'Show stack trace';
        });
      }
    }
  }

  if (!customElements.get('neutron-error-overlay')) {
    customElements.define('neutron-error-overlay', NeutronErrorOverlay);
  }

  let overlay = document.querySelector('neutron-error-overlay');
  if (!overlay) {
    overlay = document.createElement('neutron-error-overlay');
    document.body.appendChild(overlay);
  }

  window.__NEUTRON_ERROR_OVERLAY__ = {
    show(payload) {
      let el = document.querySelector('neutron-error-overlay');
      if (!el) {
        el = document.createElement('neutron-error-overlay');
        document.body.appendChild(el);
      }
      el.show(payload);
    },
    dismiss() {
      const el = document.querySelector('neutron-error-overlay');
      if (el) el.dismiss();
    }
  };
})();
`;
}
