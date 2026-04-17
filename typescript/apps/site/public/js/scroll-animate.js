// Scroll animations — re-observe after any DOM changes (survives hydration)
(function() {
  var observer = new IntersectionObserver(function(entries) {
    entries.forEach(function(entry) {
      if (entry.isIntersecting) {
        entry.target.classList.add('is-visible');
        observer.unobserve(entry.target);
      }
    });
  }, { threshold: 0.1, rootMargin: '0px 0px -40px 0px' });

  function observeAll() {
    document.querySelectorAll('[data-animate]:not(.is-visible)').forEach(function(el) {
      observer.observe(el);
    });
  }

  // Synchronously reveal any [data-animate] element already in the viewport.
  // Must run inside Neutron's view-transition callback so the captured "new"
  // snapshot has elements at opacity:1 — otherwise the crossfade shows a blank
  // hero for a frame and users see a flash, especially on back navigation.
  function revealInViewport() {
    var viewportHeight = window.innerHeight || document.documentElement.clientHeight;
    document.querySelectorAll('[data-animate]:not(.is-visible)').forEach(function(el) {
      var rect = el.getBoundingClientRect();
      if (rect.top < viewportHeight && rect.bottom > 0) {
        el.classList.add('is-visible');
        observer.unobserve(el);
      }
    });
  }

  observeAll();
  new MutationObserver(function() { observeAll(); }).observe(document.getElementById('app') || document.body, { childList: true, subtree: true });

  // Neutron dispatches this synchronously inside startViewTransition's callback.
  document.addEventListener('neutron:page-swap', revealInViewport);
})();
