# neutron.build

This directory contains the public Neutron documentation and marketing site.
The site is built before deployment and served as static files by Caddy through Teploy.

From the `typescript/` directory:

```bash
pnpm install --frozen-lockfile
pnpm --filter @neutron-build/create build
pnpm --filter @neutron-build/core build
pnpm --filter @neutron-build/cli build
pnpm --dir apps/site build
```

Then run `teploy plan --project-dir typescript/apps/site` from the repository
root and deploy with `teploy deploy --project-dir typescript/apps/site`.
The site's `.teployignore` includes the generated, gitignored `dist/` directory
in the remote Docker build context. Check the live page after deploying; a
source merge alone does not publish new documentation.
