// Terminal copy — event delegation (survives hydration)
document.addEventListener('click', function(e) {
  var btn = e.target.closest && e.target.closest('.terminal__copy');
  if (!btn) return;
  var terminal = btn.closest('.terminal');
  if (!terminal) return;
  var cmd = terminal.getAttribute('data-command');
  if (!cmd) return;
  navigator.clipboard.writeText(cmd).then(function() {
    btn.classList.add('copied');
    setTimeout(function() { btn.classList.remove('copied'); }, 1500);
  });
});
