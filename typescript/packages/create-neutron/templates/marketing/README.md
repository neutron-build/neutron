# __PROJECT_NAME__

A Neutron marketing-site starter — static pages, a blog with dynamic routes, and an interactive island.

Built with [Neutron](https://neutron.build).

## Commands

Run from the project root:

| Command | Action |
| --- | --- |
| `npm install` | Install dependencies |
| `npm run dev` | Start the dev server |
| `npm run build` | Build for production (output in `dist/`) |
| `npm run preview` | Preview the production build locally |

(Works the same with `pnpm` or `yarn`.)

## Project structure

- `src/routes/` — file-based routes: `index.tsx` -> `/`, `[id].tsx` -> dynamic params, `_layout.tsx` -> shared layout
- `src/main.tsx` — client entry and hydration
- `neutron.config.ts` — framework configuration

## Learn more

- Documentation: https://neutron.build/docs
