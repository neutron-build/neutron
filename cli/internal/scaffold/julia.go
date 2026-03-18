package scaffold

type juliaScaffolder struct{}

func (s *juliaScaffolder) Files(data *TemplateData) []TemplateFile {
	return []TemplateFile{
		{TemplatePath: "templates/julia/Project.toml.tmpl", OutputPath: "Project.toml"},
		{TemplatePath: "templates/julia/src/App.jl.tmpl", OutputPath: "src/App.jl"},
		{TemplatePath: "templates/julia/dotenv.tmpl", OutputPath: ".env"},
		{TemplatePath: "templates/julia/gitignore.tmpl", OutputPath: ".gitignore"},
		{TemplatePath: "templates/julia/migrations/001_init.up.sql.tmpl", OutputPath: "migrations/001_init.up.sql"},
		{TemplatePath: "templates/julia/migrations/001_init.down.sql.tmpl", OutputPath: "migrations/001_init.down.sql"},
	}
}
