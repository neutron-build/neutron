package scaffold

type zigScaffolder struct{}

func (s *zigScaffolder) Files(data *TemplateData) []TemplateFile {
	return []TemplateFile{
		{TemplatePath: "templates/zig/build.zig.tmpl", OutputPath: "build.zig"},
		{TemplatePath: "templates/zig/build.zig.zon.tmpl", OutputPath: "build.zig.zon"},
		{TemplatePath: "templates/zig/src/main.zig.tmpl", OutputPath: "src/main.zig"},
		{TemplatePath: "templates/zig/dotenv.tmpl", OutputPath: ".env"},
		{TemplatePath: "templates/zig/gitignore.tmpl", OutputPath: ".gitignore"},
		{TemplatePath: "templates/zig/migrations/001_init.up.sql.tmpl", OutputPath: "migrations/001_init.up.sql"},
		{TemplatePath: "templates/zig/migrations/001_init.down.sql.tmpl", OutputPath: "migrations/001_init.down.sql"},
	}
}
