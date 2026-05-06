import { getCollection } from "@neutron-build/core";

export function head() {
  return {
    title: "All units | Codex",
    description: "Browse every Codex unit. Filter by tier, section, or status.",
  };
}

interface UnitRow {
  id: string;
  title: string;
  section: string;
  chapter: string;
  tiers_present: string[];
  status: string;
}

export async function loader() {
  const units = await getCollection("units");
  const sorted = [...units].sort((a: any, b: any) =>
    a.data.id.localeCompare(b.data.id),
  );
  const rows: UnitRow[] = sorted.map((u: any) => ({
    id: u.data.id,
    title: u.data.title,
    section: u.data.section,
    chapter: u.data.chapter,
    tiers_present: u.data.tiers_present,
    status: u.data.status,
  }));
  // Compute facets
  const sections = Array.from(new Set(rows.map((r) => r.section))).sort();
  const statuses = Array.from(new Set(rows.map((r) => r.status))).sort();
  return { units: rows, sections, statuses };
}

const FILTER_INIT = `
(function () {
  function getFilters() {
    const params = new URLSearchParams(location.search);
    return {
      tier: params.get('tier') || '',
      section: params.get('section') || '',
      status: params.get('status') || '',
      q: (params.get('q') || '').toLowerCase(),
    };
  }
  function setFilter(key, value) {
    const params = new URLSearchParams(location.search);
    if (value) params.set(key, value); else params.delete(key);
    history.replaceState(null, '', '?' + params.toString());
    apply();
  }
  function apply() {
    const f = getFilters();
    document.querySelectorAll('[data-filter-tier]').forEach(function (sel) {
      sel.value = f[sel.getAttribute('data-filter-tier')] || '';
    });
    let visible = 0;
    document.querySelectorAll('tr[data-unit-row]').forEach(function (tr) {
      const tiers = (tr.dataset.tiers || '').split(',');
      const section = tr.dataset.section || '';
      const status = tr.dataset.status || '';
      const text = (tr.dataset.search || '').toLowerCase();
      const ok = (!f.tier || tiers.indexOf(f.tier) !== -1)
        && (!f.section || section === f.section)
        && (!f.status || status === f.status)
        && (!f.q || text.indexOf(f.q) !== -1);
      tr.style.display = ok ? '' : 'none';
      if (ok) visible++;
    });
    const counter = document.getElementById('filter-count');
    if (counter) counter.textContent = visible + ' shown';
  }
  document.addEventListener('change', function (e) {
    const t = e.target;
    if (!t || !t.getAttribute) return;
    const key = t.getAttribute('data-filter-tier');
    if (key) setFilter(key, t.value);
  });
  document.addEventListener('input', function (e) {
    const t = e.target;
    if (t && t.id === 'filter-q') setFilter('q', t.value);
  });
  document.addEventListener('click', function (e) {
    if (e.target && e.target.id === 'filter-clear') {
      history.replaceState(null, '', location.pathname);
      apply();
    }
  });
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', apply);
  } else { apply(); }
})();
`;

export default function UnitsIndex({ data }: { data: any }) {
  return (
    <article class="units-index">
      <h1>All units</h1>
      <p class="muted">
        {data.units.length} units total · <span id="filter-count">{data.units.length} shown</span>
      </p>

      <div class="unit-filters">
        <input
          id="filter-q"
          type="search"
          placeholder="Search title or id…"
          class="unit-filters__search"
          autocomplete="off"
        />
        <select data-filter-tier="tier" class="unit-filters__select">
          <option value="">All tiers</option>
          <option value="beginner">Beginner</option>
          <option value="intermediate">Intermediate</option>
          <option value="master">Master</option>
        </select>
        <select data-filter-tier="section" class="unit-filters__select">
          <option value="">All sections</option>
          {data.sections.map((s: string) => (
            <option value={s}>{s}</option>
          ))}
        </select>
        <select data-filter-tier="status" class="unit-filters__select">
          <option value="">All statuses</option>
          {data.statuses.map((s: string) => (
            <option value={s}>{s}</option>
          ))}
        </select>
        <button id="filter-clear" type="button" class="btn unit-filters__clear">Clear</button>
      </div>

      <table class="unit-table">
        <thead>
          <tr>
            <th>ID</th>
            <th>Title</th>
            <th>Section</th>
            <th>Tiers</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          {data.units.map((u: UnitRow) => (
            <tr
              data-unit-row
              data-tiers={u.tiers_present.join(",")}
              data-section={u.section}
              data-status={u.status}
              data-search={`${u.id} ${u.title}`}
            >
              <td><code>{u.id}</code></td>
              <td><a href={`/u/${u.id}`}>{u.title}</a></td>
              <td>{u.section} / {u.chapter}</td>
              <td>
                {u.tiers_present.map((t: string) => (
                  <span class={`tier-pill tier-pill--${t}`} title={t}>{t[0].toUpperCase()}</span>
                ))}
              </td>
              <td><span class={`badge badge--${u.status}`}>{u.status}</span></td>
            </tr>
          ))}
        </tbody>
      </table>

      <script dangerouslySetInnerHTML={{ __html: FILTER_INIT }} />
    </article>
  );
}
