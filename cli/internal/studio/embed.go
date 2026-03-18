package studio

import "embed"

// Dist contains the compiled Studio frontend (studio/dist → cli/internal/studio/dist).
// Run `npm run build` in the studio/ directory then copy dist/ here before building the CLI.
//
//go:embed all:dist
var Dist embed.FS
