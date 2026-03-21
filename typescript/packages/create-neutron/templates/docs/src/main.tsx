import { init, registerRoutes } from "neutron/client";
import { routes } from "virtual:neutron/routes";

const saved = localStorage.getItem("docs-theme") || "dark";
document.documentElement.setAttribute("data-theme", saved);

registerRoutes(routes);
void init();
