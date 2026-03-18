import { init, registerRoutes } from "neutron/client";
import { routes } from "virtual:neutron/routes";

// Theme init — runs before hydration to prevent flash
const saved = localStorage.getItem("tebian-theme") || "frappe";
document.documentElement.setAttribute("data-theme", saved);

registerRoutes(routes);
void init();
