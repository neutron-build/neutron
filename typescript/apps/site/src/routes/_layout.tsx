import "../styles/global.css";
import "../styles/components.css";
import "../styles/pages.css";
import "../styles/product-page.css";
import "../styles/typescript.css";
import "../styles/nucleus.css";
import "../styles/orm.css";
import "../styles/blog.css";
import "../styles/docs.css";
import { ViewTransitions } from "@neutron-build/core/client";
import Nav from "../components/Nav";
import Footer from "../components/Footer";

// Static site — no Preact hydration in the browser
export const config = { hydrate: false };

interface RootLayoutProps {
  children: any;
}

export function head() {
  return {
    // Static <link> tags — rendered into the SSG HTML so fonts and the favicon
    // are present without JavaScript (no FOUT, crawler- and agent-visible).
    link: [
      { rel: "icon", type: "image/svg+xml", href: "/favicon.svg" },
      { rel: "preconnect", href: "https://fonts.googleapis.com" },
      { rel: "preconnect", href: "https://fonts.gstatic.com", crossorigin: "" },
      {
        rel: "stylesheet",
        href: "https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap",
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
      <ViewTransitions />
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
