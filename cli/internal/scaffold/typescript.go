package scaffold

type typescriptScaffolder struct{}

// Files mirrors create-neutron's "basic" template
// (typescript/packages/create-neutron/templates/basic), minus its sample
// dynamic route and favicon.
func (s *typescriptScaffolder) Files(data *TemplateData) []TemplateFile {
	return []TemplateFile{
		{TemplatePath: "templates/typescript/package.json.tmpl", OutputPath: "package.json"},
		{TemplatePath: "templates/typescript/README.md.tmpl", OutputPath: "README.md"},
		{TemplatePath: "templates/typescript/AGENTS.md.tmpl", OutputPath: "AGENTS.md"},
		{TemplatePath: "templates/typescript/tsconfig.json.tmpl", OutputPath: "tsconfig.json"},
		{TemplatePath: "templates/typescript/neutron.config.ts.tmpl", OutputPath: "neutron.config.ts"},
		{TemplatePath: "templates/typescript/vite.config.ts.tmpl", OutputPath: "vite.config.ts"},
		{TemplatePath: "templates/typescript/index.html.tmpl", OutputPath: "index.html"},
		{TemplatePath: "templates/typescript/src/main.tsx.tmpl", OutputPath: "src/main.tsx"},
		{TemplatePath: "templates/typescript/src/neutron-env.d.ts.tmpl", OutputPath: "src/neutron-env.d.ts"},
		{TemplatePath: "templates/typescript/src/routes/_layout.tsx.tmpl", OutputPath: "src/routes/_layout.tsx"},
		{TemplatePath: "templates/typescript/src/routes/index.tsx.tmpl", OutputPath: "src/routes/index.tsx"},
		{TemplatePath: "templates/typescript/dotenv.tmpl", OutputPath: ".env"},
		{TemplatePath: "templates/typescript/gitignore.tmpl", OutputPath: ".gitignore"},
		{TemplatePath: "templates/typescript/migrations/001_init.up.sql.tmpl", OutputPath: "migrations/001_init.up.sql"},
		{TemplatePath: "templates/typescript/migrations/001_init.down.sql.tmpl", OutputPath: "migrations/001_init.down.sql"},
	}
}
