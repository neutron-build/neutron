import { init, registerRoutes } from "neutron/client";
import { routes } from "virtual:neutron/routes";

registerRoutes(routes);
void init();
