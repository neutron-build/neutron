package scaffold

type typescriptScaffolder struct{}

func (s *typescriptScaffolder) Files(data *TemplateData) []TemplateFile {
	return []TemplateFile{
		{TemplatePath: "templates/typescript/package.json.tmpl", OutputPath: "package.json"},
		{TemplatePath: "templates/typescript/tsconfig.json.tmpl", OutputPath: "tsconfig.json"},
		{TemplatePath: "templates/typescript/src/index.ts.tmpl", OutputPath: "src/index.ts"},
		{TemplatePath: "templates/typescript/src/routes/health.ts.tmpl", OutputPath: "src/routes/health.ts"},
		{TemplatePath: "templates/typescript/dotenv.tmpl", OutputPath: ".env"},
		{TemplatePath: "templates/typescript/gitignore.tmpl", OutputPath: ".gitignore"},
		{TemplatePath: "templates/typescript/migrations/001_init.up.sql.tmpl", OutputPath: "migrations/001_init.up.sql"},
		{TemplatePath: "templates/typescript/migrations/001_init.down.sql.tmpl", OutputPath: "migrations/001_init.down.sql"},
	}
}
