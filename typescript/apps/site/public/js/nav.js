// Nav interactivity — event delegation, intent-aware hover
(function () {
  var activePanel = null;
  var openTimer = null;
  var closeTimer = null;
  var OPEN_DELAY = 50;   // quick intent detection
  var CLOSE_DELAY = 200; // generous grace period to reach the popover

  function getNav() { return document.getElementById('main-nav'); }
  function getProducts() { return document.getElementById('nav-products'); }
  function getHighlight() { return document.getElementById('nav-highlight'); }
  function getPopover() { return document.getElementById('nav-popover'); }
  function getPopoverBg() { return document.getElementById('nav-popover-bg'); }

  // --- Sliding highlight on language/proof/modeling items ---
  document.addEventListener('mouseenter', function (e) {
    var item = e.target.closest && e.target.closest('.nav__products .nav__item');
    if (!item) return;
    var products = getProducts();
    var highlight = getHighlight();
    if (!products || !highlight) return;
    var r = item.getBoundingClientRect();
    var p = products.getBoundingClientRect();
    highlight.style.width = r.width + 'px';
    highlight.style.height = r.height + 'px';
    highlight.style.transform = 'translate(' + (r.left - p.left + products.scrollLeft) + 'px, ' + (r.top - p.top) + 'px)';
    highlight.classList.add('is-visible');
  }, true);

  document.addEventListener('mouseleave', function (e) {
    if (e.target && e.target.id === 'nav-products') {
      var highlight = getHighlight();
      if (highlight) highlight.classList.remove('is-visible');
    }
  }, true);

  // --- Dropdown popover ---
  function openPanel(name, trigger) {
    cancelClose();
    cancelOpen();
    var nav = getNav();
    var popover = getPopover();
    var popoverBg = getPopoverBg();
    if (!nav || !popover || !popoverBg) return;

    var switching = activePanel && activePanel !== name;

    nav.querySelectorAll('.nav__panel').forEach(function (p) { p.classList.remove('is-active'); });
    nav.querySelectorAll('.nav__trigger').forEach(function (t) { t.classList.remove('is-active'); });

    var panel = nav.querySelector('[data-panel="' + name + '"]');
    if (!panel) return;
    panel.classList.add('is-active');
    trigger.classList.add('is-active');

    // Measure after layout settles
    var w = panel.offsetWidth;
    var h = panel.offsetHeight;
    popoverBg.style.width = w + 'px';
    popoverBg.style.height = h + 'px';

    var triggerRect = trigger.getBoundingClientRect();
    var navRect = nav.getBoundingClientRect();
    // Center popover under the trigger, clamped to nav edges
    var triggerCenter = triggerRect.left + triggerRect.width / 2 - navRect.left;
    var left = triggerCenter - w / 2;
    left = Math.max(8, Math.min(left, navRect.width - w - 8));
    popover.style.left = left + 'px';
    popover.style.top = navRect.height + 'px';

    // If already open, just morph. Otherwise fade in.
    if (!switching) {
      popover.classList.add('is-open');
    } else {
      // already has is-open; nothing else needed — left/width transitions handle it
    }
    activePanel = name;
  }

  function scheduleOpen(name, trigger) {
    cancelOpen();
    if (activePanel === name) return;
    if (activePanel) {
      // switch immediately when another panel is already open
      openPanel(name, trigger);
      return;
    }
    openTimer = setTimeout(function () {
      openPanel(name, trigger);
    }, OPEN_DELAY);
  }

  function scheduleClose() {
    cancelClose();
    closeTimer = setTimeout(closeNow, CLOSE_DELAY);
  }

  function closeNow() {
    var popover = getPopover();
    var nav = getNav();
    if (popover) popover.classList.remove('is-open');
    if (nav) nav.querySelectorAll('.nav__trigger').forEach(function (t) { t.classList.remove('is-active'); });
    activePanel = null;
  }

  function cancelClose() { if (closeTimer) { clearTimeout(closeTimer); closeTimer = null; } }
  function cancelOpen() { if (openTimer) { clearTimeout(openTimer); openTimer = null; } }

  document.addEventListener('mouseenter', function (e) {
    var trigger = e.target.closest && e.target.closest('.nav__trigger');
    if (trigger) {
      scheduleOpen(trigger.dataset.dropdown, trigger);
      return;
    }
    if (e.target.closest && e.target.closest('.nav__popover')) {
      cancelClose();
    }
  }, true);

  document.addEventListener('mouseleave', function (e) {
    var trigger = e.target.closest && e.target.closest('.nav__trigger');
    if (trigger) {
      cancelOpen();
      scheduleClose();
      return;
    }
    if (e.target.closest && e.target.closest('.nav__popover')) {
      scheduleClose();
    }
  }, true);

  document.addEventListener('click', function (e) {
    var trigger = e.target.closest && e.target.closest('.nav__trigger');
    if (trigger) {
      e.preventDefault();
      var name = trigger.dataset.dropdown;
      if (activePanel === name) {
        closeNow();
      } else {
        openPanel(name, trigger);
      }
      return;
    }
    var nav = getNav();
    if (nav && !nav.contains(e.target)) {
      closeNow();
    }
  });

  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape' && activePanel) closeNow();
  });

  // --- Mobile drawer ---
  function getDrawer() { return document.getElementById('nav-drawer'); }
  function openDrawer() {
    var d = getDrawer();
    if (!d) return;
    d.classList.add('is-open');
    d.setAttribute('aria-hidden', 'false');
    document.body.classList.add('nav-drawer-open');
    var trigger = document.getElementById('nav-hamburger');
    if (trigger) {
      trigger.setAttribute('aria-expanded', 'true');
      trigger.setAttribute('aria-label', 'Close menu');
    }
  }
  function closeDrawer() {
    var d = getDrawer();
    if (!d) return;
    d.classList.remove('is-open');
    d.setAttribute('aria-hidden', 'true');
    document.body.classList.remove('nav-drawer-open');
    var trigger = document.getElementById('nav-hamburger');
    if (trigger) {
      trigger.setAttribute('aria-expanded', 'false');
      trigger.setAttribute('aria-label', 'Open menu');
    }
  }
  document.addEventListener('click', function (e) {
    document.querySelectorAll('.nav__ecosystem[open]').forEach(function (details) {
      if (!details.contains(e.target)) details.removeAttribute('open');
    });

    if (e.target.closest && e.target.closest('#nav-hamburger')) {
      e.preventDefault();
      var d = getDrawer();
      if (d && d.classList.contains('is-open')) closeDrawer(); else openDrawer();
      return;
    }
    if (e.target.closest && e.target.closest('[data-drawer-close]')) {
      e.preventDefault();
      closeDrawer();
    }
  });
  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') {
      document.querySelectorAll('.nav__ecosystem[open]').forEach(function (details) {
        details.removeAttribute('open');
        var summary = details.querySelector('summary');
        if (summary) summary.focus();
      });
      var d = getDrawer();
      if (d && d.classList.contains('is-open')) closeDrawer();
    }
  });
})();
