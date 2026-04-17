(function() {
  var canvas = document.getElementById('neutron-atom');
  if (!canvas) return;
  var ctx = canvas.getContext('2d');
  var animId;
  var w, h;

  function resize() {
    var rect = canvas.getBoundingClientRect();
    var dpr = Math.min(window.devicePixelRatio || 1, 2);
    w = rect.width;
    h = rect.height;
    canvas.width = w * dpr;
    canvas.height = h * dpr;
    ctx.setTransform(1, 0, 0, 1, 0, 0);
    ctx.scale(dpr, dpr);
  }

  resize();
  window.addEventListener('resize', resize);

  var colors = {
    ts:   { r: 49,  g: 120, b: 198 },
    rust: { r: 255, g: 107, b: 53  },
    mojo: { r: 168, g: 85,  b: 247 },
    nucleus: { r: 16,  g: 185, b: 129 },
    core: { r: 0,   g: 229, b: 160 }
  };

  var orbits = [
    { rx: 0.38, ry: 0.14, tilt: -15, speed: 0.6,  particles: 3, color: colors.ts,   trail: true },
    { rx: 0.32, ry: 0.12, tilt: 50,  speed: -0.45, particles: 2, color: colors.rust, trail: true },
    { rx: 0.42, ry: 0.16, tilt: 110, speed: 0.35,  particles: 3, color: colors.mojo, trail: true },
    { rx: 0.25, ry: 0.09, tilt: -60, speed: -0.55, particles: 2, color: colors.nucleus, trail: true }
  ];

  function draw(time) {
    ctx.clearRect(0, 0, w, h);
    var cx = w / 2, cy = h / 2, t = time * 0.001;

    var outerGlow = ctx.createRadialGradient(cx, cy, 0, cx, cy, 80);
    outerGlow.addColorStop(0, 'rgba(' + colors.core.r + ',' + colors.core.g + ',' + colors.core.b + ',0.2)');
    outerGlow.addColorStop(0.3, 'rgba(' + colors.core.r + ',' + colors.core.g + ',' + colors.core.b + ',0.08)');
    outerGlow.addColorStop(1, 'transparent');
    ctx.fillStyle = outerGlow;
    ctx.beginPath(); ctx.arc(cx, cy, 80, 0, Math.PI * 2); ctx.fill();

    var coreGlow = ctx.createRadialGradient(cx, cy, 0, cx, cy, 30);
    coreGlow.addColorStop(0, 'rgba(' + colors.core.r + ',' + colors.core.g + ',' + colors.core.b + ',0.5)');
    coreGlow.addColorStop(0.5, 'rgba(' + colors.core.r + ',' + colors.core.g + ',' + colors.core.b + ',0.15)');
    coreGlow.addColorStop(1, 'transparent');
    ctx.fillStyle = coreGlow;
    ctx.beginPath(); ctx.arc(cx, cy, 30, 0, Math.PI * 2); ctx.fill();

    var innerGlow = ctx.createRadialGradient(cx, cy, 0, cx, cy, 10);
    innerGlow.addColorStop(0, 'rgba(' + colors.core.r + ',' + colors.core.g + ',' + colors.core.b + ',0.9)');
    innerGlow.addColorStop(0.6, 'rgba(' + colors.core.r + ',' + colors.core.g + ',' + colors.core.b + ',0.3)');
    innerGlow.addColorStop(1, 'transparent');
    ctx.fillStyle = innerGlow;
    ctx.beginPath(); ctx.arc(cx, cy, 10, 0, Math.PI * 2); ctx.fill();

    orbits.forEach(function(orbit) {
      var tiltRad = (orbit.tilt * Math.PI) / 180;
      var rx = w * orbit.rx, ry = h * orbit.ry;

      ctx.save(); ctx.translate(cx, cy); ctx.rotate(tiltRad);
      ctx.strokeStyle = 'rgba(255,255,255,0.06)'; ctx.lineWidth = 0.75;
      ctx.beginPath(); ctx.ellipse(0, 0, rx, ry, 0, 0, Math.PI * 2); ctx.stroke();
      ctx.restore();

      for (var i = 0; i < orbit.particles; i++) {
        var angle = t * orbit.speed + (i * Math.PI * 2) / orbit.particles;
        var localX = Math.cos(angle) * rx, localY = Math.sin(angle) * ry;
        var cos = Math.cos(tiltRad), sin = Math.sin(tiltRad);
        var px = cx + localX * cos - localY * sin;
        var py = cy + localX * sin + localY * cos;
        var c = orbit.color;

        var outerP = ctx.createRadialGradient(px, py, 0, px, py, 24);
        outerP.addColorStop(0, 'rgba(' + c.r + ',' + c.g + ',' + c.b + ',0.35)');
        outerP.addColorStop(0.4, 'rgba(' + c.r + ',' + c.g + ',' + c.b + ',0.08)');
        outerP.addColorStop(1, 'transparent');
        ctx.fillStyle = outerP;
        ctx.beginPath(); ctx.arc(px, py, 24, 0, Math.PI * 2); ctx.fill();

        var glow = ctx.createRadialGradient(px, py, 0, px, py, 8);
        glow.addColorStop(0, 'rgba(' + c.r + ',' + c.g + ',' + c.b + ',0.9)');
        glow.addColorStop(0.6, 'rgba(' + c.r + ',' + c.g + ',' + c.b + ',0.3)');
        glow.addColorStop(1, 'transparent');
        ctx.fillStyle = glow;
        ctx.beginPath(); ctx.arc(px, py, 8, 0, Math.PI * 2); ctx.fill();

        ctx.fillStyle = 'rgba(' + c.r + ',' + c.g + ',' + c.b + ',1)';
        ctx.beginPath(); ctx.arc(px, py, 2.5, 0, Math.PI * 2); ctx.fill();

        if (orbit.trail) {
          for (var j = 1; j <= 10; j++) {
            var trailAngle = angle - j * 0.05 * Math.sign(orbit.speed);
            var tlx = Math.cos(trailAngle) * rx, tly = Math.sin(trailAngle) * ry;
            var tpx = cx + tlx * cos - tly * sin;
            var tpy = cy + tlx * sin + tly * cos;
            var alpha = 0.25 * (1 - j / 11);
            ctx.fillStyle = 'rgba(' + c.r + ',' + c.g + ',' + c.b + ',' + alpha + ')';
            ctx.beginPath(); ctx.arc(tpx, tpy, 2 - j * 0.15, 0, Math.PI * 2); ctx.fill();
          }
        }
      }
    });

    animId = requestAnimationFrame(draw);
  }

  var atomObserver = new IntersectionObserver(function(entries) {
    entries.forEach(function(entry) {
      if (entry.isIntersecting) { animId = requestAnimationFrame(draw); }
      else { cancelAnimationFrame(animId); }
    });
  }, { threshold: 0 });
  atomObserver.observe(canvas);
})();
