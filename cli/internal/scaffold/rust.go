package scaffold

type rustScaffolder struct{}

func (s *rustScaffolder) Files(data *TemplateData) []TemplateFile {
	return []TemplateFile{
		{TemplatePath: "templates/rust/Cargo.toml.tmpl", OutputPath: "Cargo.toml"},
		{TemplatePath: "templates/rust/README.md.tmpl", OutputPath: "README.md"},
		{TemplatePath: "templates/rust/src/main.rs.tmpl", OutputPath: "src/main.rs"},
		{TemplatePath: "templates/rust/dotenv.tmpl", OutputPath: ".env"},
		{TemplatePath: "templates/rust/gitignore.tmpl", OutputPath: ".gitignore"},
		{TemplatePath: "templates/rust/migrations/001_init.up.sql.tmpl", OutputPath: "migrations/001_init.up.sql"},
		{TemplatePath: "templates/rust/migrations/001_init.down.sql.tmpl", OutputPath: "migrations/001_init.down.sql"},
	}
}
