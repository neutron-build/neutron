#!/usr/bin/env node
import { dev } from "./commands/dev.js";
import { build } from "./commands/build.js";
import { preview } from "./commands/preview.js";
import { start } from "./commands/start.js";
import { deployCheck } from "./commands/deploy-check.js";
import { worker } from "./commands/worker.js";
import { releaseCheck } from "./commands/release-check.js";

const args = process.argv.slice(2);
const command = args[0];

async function main() {
  switch (command) {
    case "dev":
      await dev();
      break;
    case "build":
      await build();
      break;
    case "preview":
      await preview();
      break;
    case "start":
      await start();
      break;
    case "deploy-check":
      await deployCheck();
      break;
    case "release-check":
      await releaseCheck();
      break;
    case "worker":
      await worker();
      break;
    default:
      console.log(`Neutron CLI

Usage:
  neutron dev      Start development server
  neutron build    Build for production
    --preset vercel|cloudflare|docker|static
    --cloudflare-mode pages|workers
  neutron start    Start production server
  neutron preview  Preview production build
  neutron release-check
    --preset vercel|cloudflare|docker|static
    --dist dist
  neutron worker   Run background worker module
    --entry src/worker.ts
    --mode development|production
    --once
  neutron deploy-check
    --preset vercel|cloudflare|docker|static
    --dist dist
`);
      process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
