package scaffold

type pythonScaffolder struct{}

func (s *pythonScaffolder) Files(data *TemplateData) []TemplateFile {
	return []TemplateFile{
		{TemplatePath: "templates/python/pyproject.toml.tmpl", OutputPath: "pyproject.toml"},
		{TemplatePath: "templates/python/app/__init__.py.tmpl", OutputPath: "app/__init__.py"},
		{TemplatePath: "templates/python/app/main.py.tmpl", OutputPath: "app/main.py"},
		{TemplatePath: "templates/python/dotenv.tmpl", OutputPath: ".env"},
		{TemplatePath: "templates/python/gitignore.tmpl", OutputPath: ".gitignore"},
		{TemplatePath: "templates/python/migrations/001_init.up.sql.tmpl", OutputPath: "migrations/001_init.up.sql"},
		{TemplatePath: "templates/python/migrations/001_init.down.sql.tmpl", OutputPath: "migrations/001_init.down.sql"},
	}
}
