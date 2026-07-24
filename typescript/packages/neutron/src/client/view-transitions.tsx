import { h, Fragment } from "preact";

// Neutron uses browser-native view transitions for same-origin navigations so
// the browser stays in charge of routing. That's what unlocks back/forward
// bfcache (instant restore) and clean cross-document animations.
//
// Apps using <Link> or navigate() still get SPA-style view transitions via
// navigate.ts, which reads window.__NEUTRON_VIEW_TRANSITIONS__ set below.
// Apps using plain <a href> get real browser navigations — no click
// interception here, no fetch + swap, no popstate hijacking.
//
// Browser support for cross-document view transitions: Chrome 126+, Safari
// 18+. Firefox (behind dom.viewtransitions.enabled as of early 2026)
// gracefully falls back to plain navigation with no animation but full
// bfcache restore.
const TRANSITION_CSS = `
@view-transition { navigation: auto; }

html { background-color: var(--neutron-vt-bg, #000); }
::view-transition { background-color: var(--neutron-vt-bg, #000); }

main:not(main main) { view-transition-name: neutron-main; }

::view-transition-old(root),
::view-transition-new(root) {
  animation: none;
  mix-blend-mode: normal;
  height: 100%;
}
/* Hide the old root snapshot: with both snapshots frozen at opacity 1 the
   browser's plus-lighter compositing double-exposes any unnamed content
   (ghosted text). Static chrome outside neutron-main swaps instantly. */
::view-transition-old(root) { opacity: 0; }
::view-transition-group(root) { animation: none; isolation: auto; }
::view-transition-image-pair(root) { isolation: auto; }

/* Freeze the main group's geometry morph (pages of different heights would
   squish mid-transition) and opt out of plus-lighter so opacity animations
   read as a plain crossfade instead of an additive ghost. */
::view-transition-group(neutron-main) { animation: none; }
::view-transition-image-pair(neutron-main) { isolation: isolate; }
::view-transition-old(neutron-main),
::view-transition-new(neutron-main) { mix-blend-mode: normal; }

::view-transition-old(neutron-main) { animation: neutronFadeOut 150ms ease both; }
::view-transition-new(neutron-main) { animation: neutronFadeIn  150ms ease both; }
@keyframes neutronFadeOut { from { opacity: 1 } to { opacity: 0 } }
@keyframes neutronFadeIn  { from { opacity: 0 } to { opacity: 1 } }
`;

const BOOTSTRAP = `
(() => {
  if (window.__NEUTRON_VIEW_TRANSITIONS__) return;
  window.__NEUTRON_VIEW_TRANSITIONS__ = true;

  // Tag cross-document transitions with the navigation type ('push',
  // 'traverse', 'replace', 'reload') on both the outgoing and incoming
  // page so CSS can style direction via :active-view-transition-type().
  window.addEventListener('pageswap', function(event) {
    if (event.viewTransition && event.activation) {
      event.viewTransition.types.add(event.activation.navigationType);
    }
  });
  window.addEventListener('pagereveal', function(event) {
    var activation = window.navigation && window.navigation.activation;
    if (event.viewTransition && activation) {
      event.viewTransition.types.add(activation.navigationType);
    }
  });

  // Prefetch same-origin links on pointer-enter so forward clicks feel
  // instant. The browser still owns navigation, so bfcache and cross-doc
  // view transitions both remain in play.
  var seen = new Set();
  document.addEventListener('pointerenter', function(event) {
    var target = event.target;
    var element = target instanceof Element ? target : null;
    var anchor = element ? element.closest('a') : null;
    if (!anchor || !anchor.href) return;
    if (anchor.origin !== window.location.origin) return;
    if (seen.has(anchor.href)) return;
    seen.add(anchor.href);
    var link = document.createElement('link');
    link.rel = 'prefetch';
    link.href = anchor.href;
    document.head.appendChild(link);
  }, true);
})();
`;

export function ViewTransitions() {
  return h(
    Fragment,
    null,
    h("style", { dangerouslySetInnerHTML: { __html: TRANSITION_CSS } }),
    h("script", { dangerouslySetInnerHTML: { __html: BOOTSTRAP } }),
  );
}
