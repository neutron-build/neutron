// Docs search — vanilla, no framework runtime. Reads the docs collection
// embedded as JSON by SearchShell and filters it client-side. Cmd/Ctrl+K opens.
(function () {
  "use strict";

  var overlay = document.querySelector("[data-search-overlay]");
  var trigger = document.querySelector("[data-search-open]");
  var input = document.querySelector("[data-search-input]");
  var resultsEl = document.querySelector("[data-search-results]");
  var dataEl = document.querySelector("[data-search-data]");
  if (!overlay || !trigger || !input || !resultsEl || !dataEl) return;

  var entries = [];
  try {
    entries = JSON.parse(dataEl.textContent || "[]");
  } catch (e) {
    entries = [];
  }

  var results = [];
  var active = 0;

  function open() {
    overlay.hidden = false;
    overlay.classList.add("is-open");
    window.requestAnimationFrame(function () {
      input.focus();
    });
  }

  function close() {
    overlay.classList.remove("is-open");
    overlay.hidden = true;
    input.value = "";
    render([]);
  }

  function navigate(slug) {
    window.location.href = "/docs/" + slug;
  }

  function filter(q) {
    var lower = q.trim().toLowerCase();
    if (!lower) return [];
    var out = [];
    for (var i = 0; i < entries.length && out.length < 10; i++) {
      var e = entries[i];
      var title = (e.title || "").toLowerCase();
      var desc = (e.description || "").toLowerCase();
      var slug = (e.slug || "").toLowerCase();
      if (title.indexOf(lower) !== -1 || desc.indexOf(lower) !== -1 || slug.indexOf(lower) !== -1) {
        out.push(e);
      }
    }
    return out;
  }

  function render(list) {
    results = list;
    active = 0;
    if (!list.length) {
      resultsEl.hidden = true;
      resultsEl.innerHTML = "";
      return;
    }
    resultsEl.hidden = false;
    var html = "";
    for (var i = 0; i < list.length; i++) {
      var e = list[i];
      var path = String(e.slug || "").replace(/\//g, " / ");
      html +=
        '<div class="search-result" role="option" data-idx="' + i +
        '" aria-selected="' + (i === 0 ? "true" : "false") + '">' +
        '<span class="search-result-title"></span>' +
        '<span class="search-result-path"></span></div>';
    }
    resultsEl.innerHTML = html;
    // Set text via textContent to avoid injecting untrusted markup.
    var nodes = resultsEl.querySelectorAll(".search-result");
    for (var j = 0; j < nodes.length; j++) {
      nodes[j].querySelector(".search-result-title").textContent = list[j].title || list[j].slug;
      nodes[j].querySelector(".search-result-path").textContent = String(list[j].slug || "").replace(/\//g, " / ");
      nodes[j].addEventListener("click", (function (slug) {
        return function () { navigate(slug); };
      })(list[j].slug));
      nodes[j].addEventListener("mouseenter", (function (idx) {
        return function () { setActive(idx); };
      })(j));
    }
  }

  function setActive(idx) {
    active = idx;
    var nodes = resultsEl.querySelectorAll(".search-result");
    for (var i = 0; i < nodes.length; i++) {
      nodes[i].setAttribute("aria-selected", i === idx ? "true" : "false");
    }
  }

  trigger.addEventListener("click", open);

  overlay.addEventListener("click", function (e) {
    if (e.target === overlay) close();
  });

  input.addEventListener("input", function () {
    render(filter(input.value));
  });

  input.addEventListener("keydown", function (e) {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActive(Math.min(active + 1, results.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActive(Math.max(active - 1, 0));
    } else if (e.key === "Enter" && results[active]) {
      navigate(results[active].slug);
    } else if (e.key === "Escape") {
      close();
    }
  });

  document.addEventListener("keydown", function (e) {
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
      e.preventDefault();
      if (overlay.hidden) open();
      else close();
    }
  });
})();
