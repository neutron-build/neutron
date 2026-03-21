import { useState } from "preact/hooks";

export function SidebarToggle() {
  const [open, setOpen] = useState(false);

  function toggle() {
    const next = !open;
    setOpen(next);
    const sidebar = document.querySelector(".docs-sidebar");
    if (sidebar) {
      sidebar.classList.toggle("is-open", next);
    }
  }

  function close() {
    setOpen(false);
    const sidebar = document.querySelector(".docs-sidebar");
    if (sidebar) {
      sidebar.classList.remove("is-open");
    }
  }

  return (
    <>
      <button
        class="docs-mobile-toggle"
        onClick={toggle}
        aria-label={open ? "Close sidebar" : "Open sidebar"}
        aria-expanded={open}
      >
        {open ? (
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <line x1="18" y1="6" x2="6" y2="18" />
            <line x1="6" y1="6" x2="18" y2="18" />
          </svg>
        ) : (
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <line x1="3" y1="12" x2="21" y2="12" />
            <line x1="3" y1="6" x2="21" y2="6" />
            <line x1="3" y1="18" x2="21" y2="18" />
          </svg>
        )}
      </button>
      {open && <div class="docs-sidebar-overlay" onClick={close} />}
    </>
  );
}
