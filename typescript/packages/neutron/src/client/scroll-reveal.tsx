import { h } from "preact";

export interface ScrollRevealProps {
  /**
   * IntersectionObserver rootMargin. Default: "0px 0px -50px 0px"
   * Negative bottom margin means elements reveal slightly before fully entering viewport.
   */
  rootMargin?: string;
  /**
   * IntersectionObserver threshold. Default: 0.1
   */
  threshold?: number;
  /**
   * CSS selector for elements to reveal. Default: ".reveal, [data-reveal]"
   */
  selector?: string;
  /**
   * Class added to elements when they become visible. Default: "visible"
   */
  visibleClass?: string;
  /**
   * Inject default reveal CSS (opacity/transform transitions).
   * Set to false if your site provides its own reveal CSS.
   * Default: true
   */
  injectCSS?: boolean;
}

/**
 * Default reveal CSS — only injected when injectCSS is true.
 * Sites with their own design systems should set injectCSS={false}.
 */
const REVEAL_CSS = `
.reveal,
[data-reveal] {
  opacity: 0;
  transform: translateY(20px);
  transition: opacity 0.6s ease, transform 0.6s ease;
}
.reveal.visible,
[data-reveal].visible {
  opacity: 1;
  transform: none;
}
[data-stagger-children] > * {
  opacity: 0;
  transform: translateY(20px);
  transition: opacity 0.5s ease, transform 0.5s ease;
}
[data-stagger-children] > .visible,
[data-stagger-children].visible > * {
  opacity: 1;
  transform: none;
}
`;

function buildBootstrap(props: ScrollRevealProps): string {
  const rootMargin = JSON.stringify(props.rootMargin || "0px 0px -50px 0px");
  const threshold = props.threshold ?? 0.1;
  const selector = JSON.stringify(props.selector || ".reveal, [data-reveal]");
  const visibleClass = JSON.stringify(props.visibleClass || "visible");
  const injectCSS = props.injectCSS !== false;

  const cssBlock = injectCSS
    ? `var cs=document.createElement("style");cs.id="neutron-reveal-css";cs.textContent=${JSON.stringify(REVEAL_CSS)};document.head.appendChild(cs);`
    : "";

  return `(function(){
if(window.__NEUTRON_SCROLL_REVEAL__)return;
window.__NEUTRON_SCROLL_REVEAL__=true;
${cssBlock}

var observed=new WeakSet();
var observer=new IntersectionObserver(function(entries){
entries.forEach(function(entry){
if(entry.isIntersecting)entry.target.classList.add(${visibleClass});
});
},{root:null,rootMargin:${rootMargin},threshold:${threshold}});

function staggerChildren(el){
var children=el.children;
for(var i=0;i<children.length;i++){
(function(child,delay){
setTimeout(function(){child.classList.add(${visibleClass});},delay);
})(children[i],i*80);
}
}

function removeInitStyle(){
var s=document.getElementById("neutron-reveal-init");
if(s)s.remove();
}

function revealInit(){
var vh=window.innerHeight;
document.querySelectorAll(${selector}).forEach(function(el){
if(observed.has(el))return;
observed.add(el);
if(el.getBoundingClientRect().top<vh+50){
el.classList.add(${visibleClass});
if(el.hasAttribute("data-stagger-children"))staggerChildren(el);
}
observer.observe(el);
});
document.querySelectorAll("[data-stagger-children]").forEach(function(el){
if(observed.has(el))return;
observed.add(el);
if(el.getBoundingClientRect().top<vh+50)staggerChildren(el);
observer.observe(el);
});
}

if(document.readyState==="loading"){
document.addEventListener("DOMContentLoaded",revealInit);
}else{
revealInit();
}
setTimeout(revealInit,50);
setTimeout(revealInit,300);

document.addEventListener("neutron:hydrated",function(){
revealInit();
removeInitStyle();
});

document.addEventListener("neutron:page-swap",function(){
var els=document.querySelectorAll(${selector});
els.forEach(function(el){
el.style.transition="none";
el.classList.add(${visibleClass});
observed.add(el);
observer.observe(el);
});
document.querySelectorAll("[data-stagger-children]").forEach(function(el){
var children=el.children;
for(var i=0;i<children.length;i++){
children[i].style.transition="none";
children[i].classList.add(${visibleClass});
}
observed.add(el);
observer.observe(el);
});
removeInitStyle();
requestAnimationFrame(function(){requestAnimationFrame(function(){
els.forEach(function(el){el.style.transition="";});
document.querySelectorAll("[data-stagger-children]>*").forEach(function(el){el.style.transition="";});
});});
});
})();`;
}

/**
 * Drop-in component for SSR-safe scroll reveal animations.
 *
 * Add `class="reveal"` or `data-reveal` to any element to animate it on scroll.
 * Add `data-stagger-children` to a container to stagger its children.
 *
 * Handles the SSR flash problem automatically: injects a temporary override style
 * that keeps content visible until the IntersectionObserver initializes.
 *
 * Place this component in your root layout, typically after `<ViewTransitions />`.
 *
 * If your site has its own reveal CSS in a design system, pass `injectCSS={false}`
 * to only get the JS behavior and SSR flash prevention.
 *
 * @example
 * ```tsx
 * import { ScrollReveal } from "neutron/client";
 *
 * // With default CSS (good for quick prototyping):
 * <ScrollReveal />
 *
 * // With your own CSS (recommended for production sites):
 * <ScrollReveal injectCSS={false} />
 * ```
 */
export function ScrollReveal(props: ScrollRevealProps = {}) {
  return h("script", {
    dangerouslySetInnerHTML: { __html: buildBootstrap(props) },
  });
}
