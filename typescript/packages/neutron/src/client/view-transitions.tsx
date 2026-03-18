import { h } from "preact";

const TRANSITION_CSS = `
/* Prevent white flash — force black behind the transition overlay */
html { background-color: var(--neutron-vt-bg, #000); }
::view-transition { background-color: transparent; }

/* Tag the top-level <main> via CSS so it survives Preact re-renders */
main:not(main main) { view-transition-name: neutron-main; }

/* Root: instant swap (no animation) so nav/footer stay rock-stable */
::view-transition-old(root),
::view-transition-new(root) {
  animation: none;
  mix-blend-mode: normal;
}
::view-transition-group(root) { animation: none; }

/* Main content: slide animation */
::view-transition-old(neutron-main) {
  animation: neutronSlideOut 350ms cubic-bezier(0.22, 1, 0.36, 1) both;
}
::view-transition-new(neutron-main) {
  animation: neutronSlideIn 350ms cubic-bezier(0.22, 1, 0.36, 1) both;
}
/* Back navigation: reverse direction (no space before pseudo — must attach to html) */
.neutron-nav-back::view-transition-old(neutron-main) {
  animation-name: neutronSlideOutReverse;
}
.neutron-nav-back::view-transition-new(neutron-main) {
  animation-name: neutronSlideInReverse;
}
@keyframes neutronSlideOut {
  from { opacity: 1; transform: translateX(0); }
  to   { opacity: 0; transform: translateX(-80px); }
}
@keyframes neutronSlideIn {
  from { opacity: 0; transform: translateX(80px); }
  to   { opacity: 1; transform: translateX(0); }
}
@keyframes neutronSlideOutReverse {
  from { opacity: 1; transform: translateX(0); }
  to   { opacity: 0; transform: translateX(80px); }
}
@keyframes neutronSlideInReverse {
  from { opacity: 0; transform: translateX(-80px); }
  to   { opacity: 1; transform: translateX(0); }
}
`;

const BOOTSTRAP = `
(() => {
  if (window.__NEUTRON_VIEW_TRANSITIONS__) {
    return;
  }
  window.__NEUTRON_VIEW_TRANSITIONS__ = true;

  // Inject transition animation CSS
  var style = document.createElement('style');
  style.textContent = ${JSON.stringify(TRANSITION_CSS)};
  document.head.appendChild(style);

  // Tag <main> for scoped view-transition-name so nav/footer stay stable
  var main = document.querySelector('main');
  if (main) main.style.viewTransitionName = 'neutron-main';

  // Track navigation direction for directional animations
  var navigationDirection = 'forward';

  function canIntercept(anchor, event) {
    if (!anchor) return false;
    if (anchor.target && anchor.target !== "_self") return false;
    if (anchor.hasAttribute("download")) return false;
    if (event.defaultPrevented) return false;
    if (event.button !== 0) return false;
    if (event.metaKey || event.altKey || event.ctrlKey || event.shiftKey) return false;
    if (anchor.origin !== window.location.origin) return false;
    return true;
  }

  // Re-execute only inline scripts explicitly marked as safe for view transitions
  function rerunInlineScripts(container) {
    var scripts = container.querySelectorAll('script[data-neutron-inline]');
    scripts.forEach(function(oldScript) {
      var newScript = document.createElement('script');
      newScript.textContent = oldScript.textContent;
      oldScript.parentNode.replaceChild(newScript, oldScript);
    });
  }

  // SECURITY: Validate URL is same-origin to prevent open redirects
  function isSafeUrl(url) {
    try {
      var parsed = new URL(url, window.location.origin);
      return parsed.origin === window.location.origin;
    } catch {
      return false;
    }
  }

  async function handleNavigation(url) {
    // SECURITY: Block navigation to external URLs
    if (!isSafeUrl(url)) {
      console.error('[neutron] Blocked navigation to external URL:', url);
      return;
    }

    if (!document.startViewTransition) {
      window.location.href = url;
      return;
    }

    try {
      var response = await fetch(url, { headers: { Accept: "text/html" } });
      if (!response.ok) {
        window.location.href = url;
        return;
      }

      var html = await response.text();
      var nextDoc = new DOMParser().parseFromString(html, "text/html");

      // Find <main> in both current and next pages for scoped swap
      var nextMain = nextDoc.querySelector('main');
      var currentMain = document.querySelector('main');
      if (!nextMain || !currentMain) {
        // Fallback: swap entire #app
        var nextApp = nextDoc.getElementById("app");
        var currentApp = document.getElementById("app");
        if (!nextApp || !currentApp) {
          window.location.href = url;
          return;
        }
        nextMain = nextApp;
        currentMain = currentApp;
      }

      // Sync stylesheets from the next page into the current document head
      var nextStyles = nextDoc.querySelectorAll('link[rel="stylesheet"], style');
      var currentHrefs = new Set();
      document.querySelectorAll('link[rel="stylesheet"]').forEach(function(l) {
        if (l.href) currentHrefs.add(l.href);
      });
      nextStyles.forEach(function(el) {
        if (el.tagName === 'LINK') {
          if (!currentHrefs.has(el.href)) {
            document.head.appendChild(el.cloneNode(true));
          }
        } else if (el.tagName === 'STYLE') {
          document.head.appendChild(el.cloneNode(true));
        }
      });

      // Apply direction class for CSS animation targeting
      if (navigationDirection === 'back') {
        document.documentElement.classList.add('neutron-nav-back');
      } else {
        document.documentElement.classList.remove('neutron-nav-back');
      }

      // Capture references before the closure
      var swapTarget = currentMain;
      var swapSource = nextMain;

      var transition = document.startViewTransition(function() {
        document.title = nextDoc.title;
        // Only swap <main> content — nav and footer stay untouched
        // SECURITY: Use DOMParser to create inert nodes, then selectively re-execute
        // only inline scripts (blocks external <script src="..."> from executing)
        var parser = new DOMParser();
        var sanitizedDoc = parser.parseFromString(swapSource.innerHTML, 'text/html');
        swapTarget.replaceChildren(...Array.from(sanitizedDoc.body.childNodes));

        // Re-execute inline scripts in the swapped content (filters out external scripts)
        rerunInlineScripts(swapTarget);

        // Notify other components (e.g. ScrollReveal) that new content is ready
        document.dispatchEvent(new CustomEvent("neutron:page-swap"));
      });

      transition.finished.then(function() {
        document.documentElement.classList.remove('neutron-nav-back');
      });

      navigationDirection = 'forward';
      history.pushState(null, "", url);
      window.scrollTo({ top: 0, left: 0, behavior: "auto" });
    } catch(e) {
      window.location.href = url;
    }
  }

  // Handle back/forward navigation
  window.addEventListener('popstate', function() {
    if (window.__NEUTRON_ROUTER_ACTIVE__) return;
    navigationDirection = 'back';
    handleNavigation(window.location.href);
  });

  document.addEventListener("click", function(event) {
    if (window.__NEUTRON_ROUTER_ACTIVE__) return;
    var target = event.target;
    var element = target instanceof Element ? target : null;
    var anchor = element ? element.closest("a") : null;
    if (!canIntercept(anchor, event)) {
      return;
    }

    navigationDirection = 'forward';
    event.preventDefault();
    handleNavigation(anchor.href);
  });

  // Prefetch on hover for near-instant navigation
  var prefetchCache = new Set();
  document.addEventListener('pointerenter', function(event) {
    var target = event.target;
    var element = target instanceof Element ? target : null;
    var anchor = element ? element.closest('a') : null;
    if (!anchor || anchor.origin !== window.location.origin) return;
    if (prefetchCache.has(anchor.href)) return;
    prefetchCache.add(anchor.href);
    var link = document.createElement('link');
    link.rel = 'prefetch';
    link.href = anchor.href;
    document.head.appendChild(link);
  }, true);
})();
`;

export function ViewTransitions() {
  return h("script", {
    dangerouslySetInnerHTML: {
      __html: BOOTSTRAP,
    },
  });
}
