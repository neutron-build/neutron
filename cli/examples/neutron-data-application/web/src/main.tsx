import { init, registerRoutes } from "@neutron-build/core/client";
import { routes } from "virtual:neutron/routes";

registerRoutes(routes);
void init();
