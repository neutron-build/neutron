import "../styles/global.css";
import "../styles/components.css";
import "../styles/pages.css";
import "../styles/product-page.css";
import "../styles/typescript.css";
import "../styles/nucleus.css";
import "../styles/orm.css";
import "../styles/blog.css";
import "../styles/docs.css";
import Nav from "../components/Nav";
import Footer from "../components/Footer";

// Static site — no Preact hydration in the browser
export const config = { hydrate: false };

interface RootLayoutProps {
  children: any;
}

export function head() {
  return {
    headScripts: [
      {
        content: `
          (function() {
            var pc1 = document.createElement('link'); pc1.rel = 'preconnect'; pc1.href = 'https://fonts.googleapis.com'; document.head.appendChild(pc1);
            var pc2 = document.createElement('link'); pc2.rel = 'preconnect'; pc2.href = 'https://fonts.gstatic.com'; pc2.crossOrigin = ''; document.head.appendChild(pc2);
            var fl = document.createElement('link'); fl.rel = 'stylesheet'; fl.href = 'https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap'; document.head.appendChild(fl);
            var ic = document.createElement('link'); ic.rel = 'icon'; ic.type = 'image/svg+xml'; ic.href = '/favicon.svg'; document.head.appendChild(ic);
          })();
        `,
      },
    ],
    openGraph: {
      type: "website",
      image: "/og-image.png",
    },
    twitter: {
      card: "summary_large_image" as const,
      image: "/og-image.png",
    },
  } as any;
}

export default function RootLayout({ children }: RootLayoutProps) {
  return (
    <>
      <a href="#main-content" class="skip-link">Skip to content</a>
      <Nav />
      {children}
      <Footer />
      <script src="/js/scroll-animate.js" defer></script>
      <script src="/js/nav.js" defer></script>
      <script src="/js/terminal.js" defer></script>
      <script src="/js/atom.js" defer></script>
    </>
  );
}
