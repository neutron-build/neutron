package scaffold

type juliaScaffolder struct{}

func (s *juliaScaffolder) Files(data *TemplateData) []TemplateFile {
	return []TemplateFile{
		{TemplatePath: "templates/julia/Project.toml.tmpl", OutputPath: "Project.toml"},
		{TemplatePath: "templates/julia/README.md.tmpl", OutputPath: "README.md"},
		{TemplatePath: "templates/julia/src/main.jl.tmpl", OutputPath: "src/main.jl"},
		{TemplatePath: "templates/julia/dotenv.tmpl", OutputPath: ".env"},
		{TemplatePath: "templates/julia/gitignore.tmpl", OutputPath: ".gitignore"},
		{TemplatePath: "templates/julia/migrations/001_init.up.sql.tmpl", OutputPath: "migrations/001_init.up.sql"},
		{TemplatePath: "templates/julia/migrations/001_init.down.sql.tmpl", OutputPath: "migrations/001_init.down.sql"},
	}
}
