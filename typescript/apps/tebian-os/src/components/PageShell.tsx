import { Nav } from "./Nav";
import { Footer } from "./Footer";

export function PageShell({
  children,
  hideNav = false,
  hideFooter = false,
}: {
  children: preact.ComponentChildren;
  hideNav?: boolean;
  hideFooter?: boolean;
}) {
  return (
    <>
      {!hideNav && <Nav />}
      {children}
      {!hideNav && !hideFooter && <Footer />}
    </>
  );
}
