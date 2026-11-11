# __PROJECT_NAME__

A Neutron blog with [`@neutron-build/cms`](https://neutron.build/docs/content/cms): write and publish posts at `/n-admin`, and the site shows them. Visitors get no JavaScript.

Built with [Neutron](https://neutron.build). Requires Node 22.16 or newer.

## Commands

Run from the project root:

| Command | Action |
| --- | --- |
| `npm install` | Install dependencies |
| `npm run dev` | Start the dev server |
| `npm run build` | Build for production (output in `dist/`) |
| `npm start` | Run the built site |
| `npm run typecheck` | Check types |

(Works the same with `pnpm` or `yarn`.)

## Set the password

```bash
npx neutron-cms password
```

It prints a password once, and the hash to set as `CMS_PASSWORD_HASH`. Keep the password; only the hash goes in the environment. Without it, nobody can log in.

`npm start` reads `.env` (copy `.env.example`). `neutron-ts dev` does not, so pass the hash in the shell:

```bash
CMS_PASSWORD_HASH='<the hash>' npm run dev
```

Then open `/n-admin` and log in with the password.

## Where the content lives

Posts and uploaded images are in one SQLite file and one folder:

- In development: `data/site.db` and `data/media/`, created on first use.
- In production: `CMS_DATA_DIR`, normally a mounted volume. Production never creates the database, so a lost volume fails instead of showing an empty site. Run `npx neutron-cms init` once on a new volume.

`data/` and `.env` are in `.gitignore`. Back up with `npx neutron-cms backup out.db`, and copy `media/` alongside it.

## Deploy

The site runs as a Node server, not as static files:

```bash
npm run build
npm start
```

Set these in the server's environment:

| Variable | Value |
| --- | --- |
| `NODE_ENV` | `production` |
| `CMS_PASSWORD_HASH` | from `npx neutron-cms password` |
| `SITE_URL` | the public address, such as `https://example.com`. Admin saves from any other origin are refused |
| `CMS_DATA_DIR` | the volume that holds `site.db` and `media/` |

Use `/health` as the health check: it answers 503 when the database is missing. Do not build with the `docker` preset; it drops the route type the admin uses.

## Project structure

- `src/cms.ts`: what can be edited. Add lists and text fields here.
- `src/routes/index.tsx`: the post list. `src/routes/posts/[slug].tsx`: one post.
- `src/routes/n-admin/`: the admin's two route files.
- `src/routes/health.ts`: the health check.
- `src/routes/not-found.tsx`: the 404 page.

## Learn more

- CMS guide: https://neutron.build/docs/content/cms
- Neutron documentation: https://neutron.build/docs
