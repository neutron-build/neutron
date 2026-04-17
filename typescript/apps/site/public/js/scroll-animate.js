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

  // Observe now and re-observe after hydration replaces DOM
  observeAll();
  new MutationObserver(function() { observeAll(); }).observe(document.getElementById('app') || document.body, { childList: true, subtree: true });
})();
