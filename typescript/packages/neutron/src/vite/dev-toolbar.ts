import type { Route } from "../core/types.js";
import { generateErrorOverlayScript } from "./error-overlay.js";

export function generateDevToolbarModule(routes: Route[]): string {
  const routesJson = JSON.stringify(
    routes.map((r) => ({
      id: r.id,
      path: r.path,
      params: r.params,
      config: r.config,
      parentId: r.parentId,
    }))
  );

  return `
const ROUTES = ${routesJson};

const LOGO_SVG = '<svg width="16" height="16" viewBox="0 0 100 100" fill="none" xmlns="http://www.w3.org/2000/svg"><defs><linearGradient id="ng" x1="0" y1="0" x2="100" y2="100" gradientUnits="userSpaceOnUse"><stop offset="0%" stop-color="#a78bfa"/><stop offset="50%" stop-color="#818cf8"/><stop offset="100%" stop-color="#6366f1"/></linearGradient><filter id="glow"><feGaussianBlur stdDeviation="2" result="blur"/><feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge></filter></defs><circle cx="50" cy="50" r="8" fill="url(#ng)" filter="url(#glow)"/><ellipse cx="50" cy="50" rx="40" ry="14" stroke="url(#ng)" stroke-width="2.5" fill="none" opacity="0.9"><animateTransform attributeName="transform" type="rotate" from="0 50 50" to="360 50 50" dur="12s" repeatCount="indefinite"/></ellipse><ellipse cx="50" cy="50" rx="40" ry="14" stroke="url(#ng)" stroke-width="2.5" fill="none" opacity="0.7" transform="rotate(60 50 50)"><animateTransform attributeName="transform" type="rotate" from="60 50 50" to="420 50 50" dur="18s" repeatCount="indefinite"/></ellipse><ellipse cx="50" cy="50" rx="40" ry="14" stroke="url(#ng)" stroke-width="2.5" fill="none" opacity="0.5" transform="rotate(120 50 50)"><animateTransform attributeName="transform" type="rotate" from="120 50 50" to="480 50 50" dur="24s" repeatCount="indefinite"/></ellipse></svg>';

class NeutronDevToolbar extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: 'open' });
    this._routes = ROUTES;
    this._collapsed = false;
    this._activePanel = null;
    this._requests = [];
    this._errors = [];
    this._islandData = [];
    this._loadState();
  }

  connectedCallback() {
    this._buildDOM();
    this._setupHMR();
    this._setupNavListeners();
    this._scanIslands();
    this._observer = new MutationObserver(() => this._scanIslands());
    this._observer.observe(document.body, { childList: true, subtree: true });
    this._pollInterval = setInterval(() => this._scanIslands(), 2000);
  }

  disconnectedCallback() {
    if (this._observer) this._observer.disconnect();
    if (this._pollInterval) clearInterval(this._pollInterval);
  }

  // ── State persistence ──

  _loadState() {
    try {
      const s = localStorage.getItem('neutron-dev-toolbar');
      if (s) {
        const p = JSON.parse(s);
        if (typeof p.collapsed === 'boolean') this._collapsed = p.collapsed;
        if (typeof p.activePanel === 'string') this._activePanel = p.activePanel;
      }
    } catch {}
  }

  _saveState() {
    try {
      localStorage.setItem('neutron-dev-toolbar', JSON.stringify({
        collapsed: this._collapsed,
        activePanel: this._activePanel,
      }));
    } catch {}
  }

  // ── DOM construction (runs once) ──

  _buildDOM() {
    const s = this.shadowRoot;
    s.innerHTML = '';

    const style = document.createElement('style');
    style.textContent = this._getStyles();
    s.appendChild(style);

    // FAB (collapsed state)
    this._fab = document.createElement('button');
    this._fab.className = 'fab';
    this._fab.title = 'Open Neutron Dev Toolbar';
    this._fab.innerHTML = LOGO_SVG;
    this._fab.addEventListener('click', () => {
      this._collapsed = false;
      this._saveState();
      this._syncVisibility();
    });
    s.appendChild(this._fab);

    // Floating wrapper (expanded state)
    this._wrapper = document.createElement('div');
    this._wrapper.className = 'wrapper';

    // Panel
    this._panelEl = document.createElement('div');
    this._panelEl.className = 'panel-container';
    this._panelEl.innerHTML = '<div class="panel-inner" id="panel-inner"></div>';
    this._wrapper.appendChild(this._panelEl);

    // Bottom bar
    this._barEl = document.createElement('div');
    this._barEl.className = 'bottom-bar';
    this._wrapper.appendChild(this._barEl);

    s.appendChild(this._wrapper);

    // Build bar content with event listeners
    this._buildBar();
    this._syncVisibility();

    // Entrance animation
    requestAnimationFrame(() => this._wrapper.classList.add('entered'));
  }

  _buildBar() {
    const bar = this._barEl;
    bar.innerHTML = '';

    // Left side
    const left = document.createElement('div');
    left.className = 'bar-left';

    const logo = document.createElement('span');
    logo.className = 'logo';
    logo.innerHTML = LOGO_SVG + '<span class="logo-text">Neutron</span>';
    left.appendChild(logo);

    left.appendChild(this._makeSep());

    this._routeSpan = document.createElement('span');
    this._routeSpan.className = 'route-display';
    left.appendChild(this._routeSpan);

    left.appendChild(this._makeSep());

    this._timeSpan = document.createElement('span');
    this._timeSpan.className = 'stat';
    left.appendChild(this._timeSpan);

    left.appendChild(this._makeSep());

    this._islandSpan = document.createElement('span');
    this._islandSpan.className = 'stat';
    left.appendChild(this._islandSpan);

    bar.appendChild(left);

    // Right side
    const right = document.createElement('div');
    right.className = 'bar-right';

    const tabs = ['routes', 'perf', 'islands', 'errors'];
    const icons = {
      routes: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M12 1v6m0 6v6m-7-7h6m6 0h6"/></svg>',
      perf: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>',
      islands: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z"/></svg>',
      errors: '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>',
    };

    this._tabBtns = {};
    for (const t of tabs) {
      const btn = document.createElement('button');
      btn.className = 'tab-btn' + (this._activePanel === t ? ' active' : '');
      btn.dataset.panel = t;
      btn.innerHTML = icons[t] + ' ' + t.charAt(0).toUpperCase() + t.slice(1);
      if (t === 'errors') {
        this._errorBadge = document.createElement('span');
        this._errorBadge.className = 'badge';
        this._errorBadge.style.display = 'none';
        btn.appendChild(this._errorBadge);
      }
      btn.addEventListener('click', () => this._togglePanel(t));
      right.appendChild(btn);
      this._tabBtns[t] = btn;
    }

    const collapse = document.createElement('button');
    collapse.className = 'collapse-btn';
    collapse.title = 'Collapse';
    collapse.innerHTML = '<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"/></svg>';
    collapse.addEventListener('click', () => {
      this._collapsed = true;
      this._activePanel = null;
      this._saveState();
      this._syncVisibility();
    });
    right.appendChild(collapse);

    bar.appendChild(right);

    this._refreshBarStats();
  }

  _makeSep() {
    const el = document.createElement('span');
    el.className = 'separator';
    return el;
  }

  // ── Panel toggling (no DOM rebuild) ──

  _togglePanel(name) {
    if (this._activePanel === name) {
      this._activePanel = null;
    } else {
      this._activePanel = name;
      this._refreshPanel(name);
    }
    this._saveState();
    this._syncPanelVisibility();
    this._syncTabActive();
  }

  _syncVisibility() {
    this._fab.style.display = this._collapsed ? 'flex' : 'none';
    this._wrapper.style.display = this._collapsed ? 'none' : 'flex';
    if (!this._collapsed) {
      this._refreshBarStats();
      this._syncPanelVisibility();
      this._syncTabActive();
    }
  }

  _syncPanelVisibility() {
    const open = this._activePanel !== null;
    this._panelEl.classList.toggle('open', open);
    if (open) {
      this._refreshPanel(this._activePanel);
    }
  }

  _syncTabActive() {
    for (const [name, btn] of Object.entries(this._tabBtns)) {
      btn.classList.toggle('active', name === this._activePanel);
    }
  }

  // ── Data helpers ──

  _getCurrentRoute() {
    const ids = window.__NEUTRON_ACTIVE_ROUTE_IDS__;
    if (ids && ids.length > 0) {
      const found = this._routes.find(r => r.id === ids[ids.length - 1]);
      if (found) return found;
    }
    const pathname = window.location.pathname;
    for (const r of this._routes) {
      if (r.path === pathname) return r;
    }
    return null;
  }

  _scanIslands() {
    const islands = document.querySelectorAll('neutron-island');
    const data = [];
    for (const el of islands) {
      data.push({
        component: el.getAttribute('data-component') || '(unknown)',
        directive: el.getAttribute('data-client') || '(none)',
        hydrated: !!el.__neutronHydrated,
      });
    }
    const changed = JSON.stringify(data) !== JSON.stringify(this._islandData);
    this._islandData = data;
    if (changed && this._activePanel === 'islands') {
      this._refreshPanel('islands');
    }
    this._refreshBarStats();
  }

  _esc(str) {
    return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  // ── HMR + nav listeners ──

  _setupHMR() {
    if (import.meta.hot) {
      import.meta.hot.on('neutron:dev-toolbar:request', (data) => {
        this._requests.unshift(data);
        if (this._requests.length > 20) this._requests.length = 20;
        this._refreshBarStats();
        if (this._activePanel === 'perf') this._refreshPanel('perf');
      });

      import.meta.hot.on('neutron:routes-updated', (data) => {
        this._routes = data;
        this._refreshBarStats();
        if (this._activePanel === 'routes') this._refreshPanel('routes');
      });

      import.meta.hot.on('neutron:dev-toolbar:error', (data) => {
        this._errors.unshift(data);
        if (this._errors.length > 50) this._errors.length = 50;
        this._refreshBarStats();
        if (this._activePanel === 'errors') this._refreshPanel('errors');
        // Show the error overlay
        if (window.__NEUTRON_ERROR_OVERLAY__) {
          window.__NEUTRON_ERROR_OVERLAY__.show(data);
        }
      });

      import.meta.hot.on('neutron:error-resolved', () => {
        if (window.__NEUTRON_ERROR_OVERLAY__) {
          window.__NEUTRON_ERROR_OVERLAY__.dismiss();
        }
      });

      import.meta.hot.on('vite:beforeUpdate', () => {
        if (window.__NEUTRON_ERROR_OVERLAY__) {
          window.__NEUTRON_ERROR_OVERLAY__.dismiss();
        }
      });
    }
  }

  _setupNavListeners() {
    const refresh = () => setTimeout(() => {
      this._scanIslands();
      this._refreshBarStats();
      if (this._activePanel === 'routes') this._refreshPanel('routes');
    }, 50);
    window.addEventListener('neutron:navigation', refresh);
    window.addEventListener('popstate', refresh);
  }

  // ── Bar stats (lightweight updates, no rebuild) ──

  _refreshBarStats() {
    if (this._collapsed || !this._routeSpan) return;

    const route = this._getCurrentRoute();
    const routePath = route ? route.path : window.location.pathname;
    const routeLabel = route ? '' : '<span class="dim"> (no match)</span>';
    this._routeSpan.innerHTML = this._esc(routePath) + routeLabel;

    const lastReq = this._requests[0];
    const reqTime = lastReq ? lastReq.totalMs.toFixed(0) + 'ms' : '--';
    const timeVal = lastReq ? lastReq.totalMs : -1;
    const timeClass = timeVal > 200 ? 'stat stat-slow' : timeVal > 50 ? 'stat stat-mid' : 'stat stat-fast';
    this._timeSpan.className = timeVal < 0 ? 'stat' : timeClass;
    this._timeSpan.textContent = reqTime;

    this._islandSpan.textContent = this._islandData.length + ' island' + (this._islandData.length !== 1 ? 's' : '');

    if (this._errorBadge) {
      const n = this._errors.length;
      this._errorBadge.textContent = n;
      this._errorBadge.style.display = n > 0 ? 'inline-flex' : 'none';
    }
  }

  // ── Panel content rendering ──

  _refreshPanel(name) {
    const inner = this._panelEl.querySelector('#panel-inner');
    if (!inner) return;
    switch (name) {
      case 'routes': inner.innerHTML = this._renderRoutes(); break;
      case 'perf': inner.innerHTML = this._renderPerf(); break;
      case 'islands': inner.innerHTML = this._renderIslands(); break;
      case 'errors': inner.innerHTML = this._renderErrors(); break;
    }
  }

  _renderRoutes() {
    const cur = this._getCurrentRoute();
    if (this._routes.length === 0) {
      return '<div class="empty">No routes registered yet.</div>';
    }
    let html = '<div class="ph"><span class="pt">Routes</span><span class="pc">' + this._routes.length + '</span></div>';
    for (const r of this._routes) {
      const active = cur && r.id === cur.id;
      const params = r.params.length > 0 ? '<span class="rp">' + r.params.map(p => ':' + p).join(' ') + '</span>' : '';
      const parent = r.parentId ? '<span class="rprt">&larr; ' + this._esc(r.parentId) + '</span>' : '';
      html += '<div class="row' + (active ? ' current' : '') + '">'
        + (active ? '<div class="ci"></div>' : '')
        + '<span class="rpath">' + this._esc(r.path || '/') + '</span>'
        + params
        + '<span class="tag tag-' + (r.config?.mode || 'app') + '">' + (r.config?.mode || 'app') + '</span>'
        + parent
        + '<span class="rid">' + this._esc(r.id) + '</span>'
        + '</div>';
    }
    return html;
  }

  _renderPerf() {
    if (this._requests.length === 0) {
      return '<div class="empty">Navigate to a route to see performance data.</div>';
    }
    const latest = this._requests[0];
    const tc = latest.totalMs > 200 ? '#f87171' : latest.totalMs > 50 ? '#fbbf24' : '#4ade80';
    const rc = (latest.renderMs || 0) > 50 ? '#fbbf24' : '#4ade80';
    let html = '<div class="ph"><span class="pt">Performance</span></div>';
    html += '<div class="pcards">';
    html += '<div class="pcard"><div class="pcl">Total</div><div class="pcv" style="color:' + tc + '">' + latest.totalMs.toFixed(1) + '<span class="pcu">ms</span></div></div>';
    html += '<div class="pcard"><div class="pcl">Render</div><div class="pcv" style="color:' + rc + '">' + (latest.renderMs != null ? latest.renderMs.toFixed(1) : '--') + '<span class="pcu">ms</span></div></div>';
    html += '<div class="pcard"><div class="pcl">Loaders</div><div class="pcv" style="color:#818cf8">' + (latest.loaders || []).length + '</div></div>';
    html += '</div>';

    if (latest.loaders && latest.loaders.length > 0) {
      html += '<div class="sl">Loader Waterfall</div>';
      for (const l of latest.loaders) {
        const pct = latest.totalMs > 0 ? Math.max(4, l.ms / latest.totalMs * 100) : 4;
        const c = l.ms > 100 ? '#f87171' : l.ms > 30 ? '#fbbf24' : '#818cf8';
        html += '<div class="lrow"><span class="lname">' + this._esc(l.routeId) + '</span><div class="ltrack"><div class="lfill" style="width:' + pct + '%;background:linear-gradient(90deg,' + c + ',' + c + 'cc)"></div></div><span class="ltime">' + l.ms.toFixed(1) + 'ms</span></div>';
      }
    }

    html += '<div class="sl">History</div><table class="htable"><thead><tr><th>Path</th><th>Total</th><th>Render</th></tr></thead><tbody>';
    for (const r of this._requests.slice(0, 10)) {
      const cls = r.totalMs > 200 ? ' class="rslow"' : r.totalMs > 50 ? ' class="rmid"' : '';
      html += '<tr' + cls + '><td class="hpath">' + this._esc(r.pathname) + '</td><td class="hnum">' + r.totalMs.toFixed(0) + 'ms</td><td class="hnum">' + (r.renderMs != null ? r.renderMs.toFixed(0) + 'ms' : '--') + '</td></tr>';
    }
    html += '</tbody></table>';
    return html;
  }

  _renderIslands() {
    if (this._islandData.length === 0) {
      return '<div class="empty">No islands on this page.</div>';
    }
    const hc = this._islandData.filter(i => i.hydrated).length;
    const pc = this._islandData.length - hc;
    let html = '<div class="ph"><span class="pt">Islands</span><div class="isum">';
    if (hc > 0) html += '<span class="ibadge ih">' + hc + ' hydrated</span>';
    if (pc > 0) html += '<span class="ibadge ip">' + pc + ' pending</span>';
    html += '</div></div>';

    for (const i of this._islandData) {
      html += '<div class="row"><div class="hdot-wrap ' + (i.hydrated ? 'h' : 'p') + '"><div class="hdot"></div></div><div class="iinfo"><span class="iname">' + this._esc(i.component) + '</span><span class="istatus">' + (i.hydrated ? 'Hydrated' : 'Pending') + '</span></div><span class="tag tag-app">' + this._esc(i.directive) + '</span></div>';
    }
    return html;
  }

  _renderErrors() {
    if (this._errors.length === 0) {
      return '<div class="empty">No SSR errors recorded.</div>';
    }
    let html = '<div class="ph"><span class="pt">Errors</span><span class="pc ec">' + this._errors.length + '</span></div>';
    for (const e of this._errors) {
      const time = e.timestamp ? new Date(e.timestamp).toLocaleTimeString() : '';
      html += '<div class="erow"><div class="ehdr"><span class="emsg">' + this._esc(e.message || 'Unknown error') + '</span>' + (time ? '<span class="etime">' + time + '</span>' : '') + '</div>';
      if (e.source) html += '<div class="esrc">' + this._esc(e.source) + '</div>';
      if (e.stack) html += '<details class="edet"><summary>Stack trace</summary><pre class="estk">' + this._esc(e.stack) + '</pre></details>';
      html += '</div>';
    }
    return html;
  }

  // ── Styles ──

  _getStyles() {
    return \`
      @keyframes fadeUp {
        from { opacity: 0; transform: translateY(8px); }
        to { opacity: 1; transform: translateY(0); }
      }
      @keyframes barGrow { from { width: 0 !important; } }
      @keyframes dotPulse {
        0%, 100% { transform: scale(1); opacity: 1; }
        50% { transform: scale(1.5); opacity: 0.6; }
      }
      @keyframes fabPulse {
        0%, 100% { box-shadow: 0 0 0 0 rgba(129, 140, 248, 0.25), 0 4px 20px rgba(0,0,0,0.4); }
        50% { box-shadow: 0 0 0 6px rgba(129, 140, 248, 0), 0 4px 20px rgba(0,0,0,0.4); }
      }

      :host {
        position: fixed;
        bottom: 0;
        left: 0;
        right: 0;
        z-index: 2147483647;
        font-family: 'Inter', 'SF Pro Display', -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
        font-size: 13px;
        line-height: 1.5;
        color: #e2e8f0;
        pointer-events: none;
        display: flex;
        justify-content: center;
        padding-bottom: 12px;
      }

      * { box-sizing: border-box; margin: 0; padding: 0; }

      /* ── FAB ── */
      .fab {
        position: fixed;
        bottom: 16px;
        right: 16px;
        width: 40px;
        height: 40px;
        border-radius: 12px;
        background: rgba(15, 23, 42, 0.9);
        backdrop-filter: blur(16px) saturate(180%);
        -webkit-backdrop-filter: blur(16px) saturate(180%);
        border: 1px solid rgba(129, 140, 248, 0.2);
        cursor: pointer;
        display: flex;
        align-items: center;
        justify-content: center;
        pointer-events: auto;
        animation: fabPulse 3s ease-in-out infinite;
        transition: all 0.2s cubic-bezier(0.16, 1, 0.3, 1);
      }
      .fab:hover {
        transform: scale(1.08);
        border-color: rgba(129, 140, 248, 0.4);
      }
      .fab:active { transform: scale(0.95); }

      /* ── Floating wrapper ── */
      .wrapper {
        display: flex;
        flex-direction: column;
        width: 640px;
        max-width: calc(100vw - 32px);
        pointer-events: auto;
        opacity: 0;
        transform: translateY(12px);
        transition: opacity 0.3s, transform 0.3s cubic-bezier(0.16, 1, 0.3, 1);
        filter: drop-shadow(0 8px 32px rgba(0, 0, 0, 0.45)) drop-shadow(0 0 1px rgba(129, 140, 248, 0.15));
      }
      .wrapper.entered { opacity: 1; transform: translateY(0); }

      /* ── Bottom bar ── */
      .bottom-bar {
        display: flex;
        align-items: center;
        justify-content: space-between;
        height: 40px;
        padding: 0 6px 0 14px;
        background: rgba(15, 23, 42, 0.92);
        backdrop-filter: blur(24px) saturate(180%);
        -webkit-backdrop-filter: blur(24px) saturate(180%);
        border: 1px solid rgba(129, 140, 248, 0.1);
        border-radius: 14px;
        gap: 4px;
      }

      .bar-left, .bar-right { display: flex; align-items: center; gap: 6px; }
      .bar-left { flex: 1; min-width: 0; overflow: hidden; }

      .logo {
        display: flex;
        align-items: center;
        gap: 6px;
        user-select: none;
        flex-shrink: 0;
      }
      .logo-text {
        font-weight: 700;
        font-size: 12px;
        background: linear-gradient(135deg, #a78bfa, #818cf8, #6366f1);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        letter-spacing: -0.02em;
      }

      .separator {
        width: 1px;
        height: 14px;
        background: rgba(148, 163, 184, 0.15);
        flex-shrink: 0;
      }

      .dim { color: #64748b; }
      .route-display {
        font-size: 11px;
        font-weight: 500;
        color: #94a3b8;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        min-width: 0;
      }
      .stat {
        font-size: 11px;
        font-weight: 600;
        color: #94a3b8;
        white-space: nowrap;
        font-variant-numeric: tabular-nums;
        flex-shrink: 0;
      }
      .stat-fast { color: #4ade80; }
      .stat-mid { color: #fbbf24; }
      .stat-slow { color: #f87171; }

      /* ── Tab buttons ── */
      .tab-btn {
        display: flex;
        align-items: center;
        gap: 4px;
        background: transparent;
        border: 1px solid transparent;
        color: #64748b;
        cursor: pointer;
        padding: 4px 8px;
        border-radius: 8px;
        font-size: 11px;
        font-weight: 500;
        font-family: inherit;
        transition: all 0.15s;
        white-space: nowrap;
        flex-shrink: 0;
      }
      .tab-btn svg { opacity: 0.5; transition: opacity 0.15s; }
      .tab-btn:hover { color: #cbd5e1; background: rgba(129, 140, 248, 0.06); }
      .tab-btn:hover svg { opacity: 0.8; }
      .tab-btn.active {
        color: #e2e8f0;
        background: rgba(129, 140, 248, 0.12);
        border-color: rgba(129, 140, 248, 0.18);
      }
      .tab-btn.active svg { opacity: 1; stroke: #a78bfa; }

      .badge {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        background: linear-gradient(135deg, #ef4444, #dc2626);
        color: #fff;
        font-size: 9px;
        font-weight: 700;
        padding: 0 4px;
        min-width: 15px;
        height: 14px;
        border-radius: 7px;
        margin-left: 3px;
        box-shadow: 0 0 6px rgba(239, 68, 68, 0.3);
      }

      .collapse-btn {
        display: flex;
        align-items: center;
        justify-content: center;
        background: transparent;
        border: none;
        color: #475569;
        cursor: pointer;
        width: 26px;
        height: 26px;
        border-radius: 8px;
        transition: all 0.15s;
        flex-shrink: 0;
      }
      .collapse-btn:hover { color: #e2e8f0; background: rgba(129, 140, 248, 0.08); }

      /* ── Panel ── */
      .panel-container {
        max-height: 0;
        overflow: hidden;
        opacity: 0;
        background: rgba(15, 23, 42, 0.94);
        backdrop-filter: blur(24px) saturate(180%);
        -webkit-backdrop-filter: blur(24px) saturate(180%);
        border: 1px solid rgba(129, 140, 248, 0.1);
        border-bottom: none;
        border-radius: 14px 14px 0 0;
        transition: max-height 0.25s cubic-bezier(0.16, 1, 0.3, 1), opacity 0.2s;
      }
      .panel-container.open {
        max-height: 340px;
        opacity: 1;
        overflow-y: auto;
      }
      .panel-container.open + .bottom-bar {
        border-radius: 0 0 14px 14px;
        border-top-color: rgba(129, 140, 248, 0.06);
      }

      .panel-inner { padding: 14px 16px 10px; }

      /* Scrollbar */
      .panel-container::-webkit-scrollbar { width: 5px; }
      .panel-container::-webkit-scrollbar-track { background: transparent; }
      .panel-container::-webkit-scrollbar-thumb { background: rgba(129, 140, 248, 0.15); border-radius: 3px; }
      .panel-container::-webkit-scrollbar-thumb:hover { background: rgba(129, 140, 248, 0.3); }

      /* ── Shared panel elements ── */
      .ph { display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px; padding-bottom: 8px; border-bottom: 1px solid rgba(129, 140, 248, 0.06); }
      .pt { font-weight: 700; font-size: 13px; color: #f1f5f9; letter-spacing: -0.01em; }
      .pc { font-size: 10px; font-weight: 600; color: #818cf8; background: rgba(129, 140, 248, 0.1); padding: 1px 7px; border-radius: 8px; }
      .ec { color: #f87171; background: rgba(248, 113, 113, 0.1); }
      .sl { font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.06em; color: #475569; margin: 12px 0 6px; }
      .empty { color: #475569; text-align: center; padding: 24px 12px; font-size: 12px; }

      /* ── Routes ── */
      .row {
        display: flex;
        align-items: center;
        gap: 7px;
        padding: 5px 8px;
        border-radius: 7px;
        font-size: 12px;
        transition: background 0.12s;
        position: relative;
      }
      .row:hover { background: rgba(129, 140, 248, 0.05); }
      .row.current { background: rgba(129, 140, 248, 0.08); }
      .ci {
        position: absolute;
        left: 0; top: 50%;
        transform: translateY(-50%);
        width: 3px; height: 55%;
        border-radius: 0 3px 3px 0;
        background: linear-gradient(180deg, #a78bfa, #6366f1);
      }
      .rpath {
        font-weight: 600;
        color: #e2e8f0;
        font-family: ui-monospace, 'Cascadia Code', 'Source Code Pro', Menlo, Consolas, monospace;
        font-size: 11px;
      }
      .rp { font-size: 10px; color: #818cf8; font-family: ui-monospace, Menlo, Consolas, monospace; }
      .rprt { font-size: 10px; color: #475569; }
      .rid { margin-left: auto; font-size: 10px; color: #3f4f6a; font-family: ui-monospace, Menlo, Consolas, monospace; }
      .tag {
        font-size: 9px; padding: 1px 5px; border-radius: 4px;
        font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em;
        flex-shrink: 0;
      }
      .tag-app { background: rgba(99, 102, 241, 0.12); color: #a5b4fc; }
      .tag-static { background: rgba(34, 197, 94, 0.1); color: #86efac; }

      /* ── Perf ── */
      .pcards { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin-bottom: 4px; }
      .pcard {
        background: rgba(30, 41, 59, 0.5);
        border: 1px solid rgba(129, 140, 248, 0.06);
        border-radius: 9px;
        padding: 8px 10px;
        text-align: center;
        transition: border-color 0.15s;
      }
      .pcard:hover { border-color: rgba(129, 140, 248, 0.15); }
      .pcl { font-size: 9px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.06em; color: #475569; margin-bottom: 2px; }
      .pcv { font-size: 20px; font-weight: 700; font-variant-numeric: tabular-nums; letter-spacing: -0.02em; }
      .pcu { font-size: 11px; font-weight: 500; opacity: 0.5; margin-left: 1px; }

      .lrow { display: flex; align-items: center; gap: 8px; padding: 3px 0; font-size: 11px; }
      .lname { width: 110px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-family: ui-monospace, Menlo, Consolas, monospace; color: #94a3b8; font-size: 10px; }
      .ltrack { flex: 1; height: 6px; background: rgba(30, 41, 59, 0.7); border-radius: 3px; overflow: hidden; }
      .lfill { height: 100%; border-radius: 3px; animation: barGrow 0.5s cubic-bezier(0.16, 1, 0.3, 1) both; }
      .ltime { width: 50px; text-align: right; font-variant-numeric: tabular-nums; font-weight: 500; color: #64748b; font-size: 10px; }

      .htable { width: 100%; border-collapse: collapse; font-size: 11px; }
      .htable th { text-align: left; padding: 4px 8px; font-size: 9px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.06em; color: #3f4f6a; border-bottom: 1px solid rgba(129, 140, 248, 0.06); }
      .htable td { padding: 3px 8px; border-bottom: 1px solid rgba(30, 41, 59, 0.4); }
      .htable tbody tr:hover td { background: rgba(129, 140, 248, 0.03); }
      .hpath { font-family: ui-monospace, Menlo, Consolas, monospace; font-size: 10px; color: #94a3b8; }
      .hnum { font-variant-numeric: tabular-nums; font-weight: 500; color: #64748b; }
      .rslow td:nth-child(2) { color: #f87171; }
      .rmid td:nth-child(2) { color: #fbbf24; }

      /* ── Islands ── */
      .isum { display: flex; gap: 5px; }
      .ibadge { font-size: 9px; font-weight: 600; padding: 1px 7px; border-radius: 8px; }
      .ibadge.ih { background: rgba(74, 222, 128, 0.08); color: #4ade80; }
      .ibadge.ip { background: rgba(251, 191, 36, 0.08); color: #fbbf24; }

      .hdot-wrap {
        width: 24px; height: 24px; border-radius: 50%;
        display: flex; align-items: center; justify-content: center; flex-shrink: 0;
      }
      .hdot-wrap.h { background: rgba(74, 222, 128, 0.08); }
      .hdot-wrap.p { background: rgba(251, 191, 36, 0.08); }
      .hdot { width: 7px; height: 7px; border-radius: 50%; }
      .hdot-wrap.h .hdot { background: #4ade80; box-shadow: 0 0 5px rgba(74, 222, 128, 0.4); }
      .hdot-wrap.p .hdot { background: #fbbf24; box-shadow: 0 0 5px rgba(251, 191, 36, 0.3); animation: dotPulse 2s ease-in-out infinite; }

      .iinfo { display: flex; flex-direction: column; gap: 0; flex: 1; min-width: 0; }
      .iname { font-weight: 600; font-size: 12px; color: #e2e8f0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
      .istatus { font-size: 10px; color: #475569; }

      /* ── Errors ── */
      .erow {
        padding: 10px 12px; border-radius: 9px;
        background: rgba(239, 68, 68, 0.04);
        border: 1px solid rgba(239, 68, 68, 0.08);
        margin-bottom: 6px;
        transition: border-color 0.15s;
      }
      .erow:hover { border-color: rgba(239, 68, 68, 0.18); }
      .ehdr { display: flex; align-items: flex-start; justify-content: space-between; gap: 8px; margin-bottom: 2px; }
      .emsg { font-weight: 600; color: #fca5a5; font-size: 12px; line-height: 1.4; }
      .etime { font-size: 9px; color: #475569; white-space: nowrap; font-variant-numeric: tabular-nums; padding-top: 2px; }
      .esrc { font-size: 10px; color: #475569; margin-bottom: 4px; font-family: ui-monospace, Menlo, Consolas, monospace; }
      .edet { margin-top: 4px; }
      .edet summary { font-size: 10px; color: #475569; cursor: pointer; user-select: none; transition: color 0.12s; }
      .edet summary:hover { color: #94a3b8; }
      .estk {
        font-size: 10px; color: #475569; white-space: pre-wrap; word-break: break-all;
        max-height: 120px; overflow-y: auto; margin-top: 4px; padding: 8px 10px;
        background: rgba(15, 23, 42, 0.5); border-radius: 7px; border: 1px solid rgba(30, 41, 59, 0.6);
        font-family: ui-monospace, Menlo, Consolas, monospace; line-height: 1.5;
      }
      .estk::-webkit-scrollbar { width: 4px; }
      .estk::-webkit-scrollbar-track { background: transparent; }
      .estk::-webkit-scrollbar-thumb { background: rgba(100, 116, 139, 0.2); border-radius: 2px; }
    \`;
  }
}

if (!customElements.get('neutron-dev-toolbar')) {
  customElements.define('neutron-dev-toolbar', NeutronDevToolbar);
}

if (!document.querySelector('neutron-dev-toolbar')) {
  document.body.appendChild(document.createElement('neutron-dev-toolbar'));
}

if (import.meta.hot) {
  import.meta.hot.accept();
}

// ── Error Overlay ──
${generateErrorOverlayScript()}
`;
}
