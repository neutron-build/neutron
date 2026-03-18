package scaffold

type goScaffolder struct{}

func (s *goScaffolder) Files(data *TemplateData) []TemplateFile {
	return []TemplateFile{
		{TemplatePath: "templates/go/go.mod.tmpl", OutputPath: "go.mod"},
		{TemplatePath: "templates/go/cmd/server/main.go.tmpl", OutputPath: "cmd/server/main.go"},
		{TemplatePath: "templates/go/internal/handler/health.go.tmpl", OutputPath: "internal/handler/health.go"},
		{TemplatePath: "templates/go/internal/model/model.go.tmpl", OutputPath: "internal/model/model.go"},
		{TemplatePath: "templates/go/dotenv.example.tmpl", OutputPath: ".env.example"},
		{TemplatePath: "templates/go/gitignore.tmpl", OutputPath: ".gitignore"},
		{TemplatePath: "templates/go/migrations/001_init.up.sql.tmpl", OutputPath: "migrations/001_init.up.sql"},
		{TemplatePath: "templates/go/migrations/001_init.down.sql.tmpl", OutputPath: "migrations/001_init.down.sql"},
	}
}
