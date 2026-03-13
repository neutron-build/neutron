"""Neutron CLI — Python-specific dev server, routes, and utilities.

Uses cyclopts for type-hint CLI parsing and Rich for terminal output.

Usage::

    python -m neutron dev
    python -m neutron run app:app
    python -m neutron routes app:app
    python -m neutron migrate app:app

NOTE: The global ``neutron`` command is the universal CLI (Go binary).
Python-specific commands are invoked via ``python -m neutron``.
"""

from __future__ import annotations

import importlib
import os
import sys
import textwrap
from pathlib import Path
from typing import Annotated

import cyclopts

app = cyclopts.App(
    name="neutron",
    help="Neutron — The AI application development framework for Python.",
    version_flags=["--version", "-V"],
    version="0.1.0",
)


# ---------------------------------------------------------------------------
# neutron new <name>
# ---------------------------------------------------------------------------

@app.command
def new(
    name: Annotated[str, cyclopts.Parameter(help="Project name")],
    *,
    with_ai: Annotated[bool, cyclopts.Parameter(help="Include AI module scaffolding")] = False,
    with_nucleus: Annotated[bool, cyclopts.Parameter(help="Include Nucleus database setup")] = True,
) -> None:
    """Scaffold a new Neutron project."""
    try:
        from rich.console import Console
        console = Console()
        _print = console.print
    except ImportError:
        _print = print  # type: ignore[assignment]

    project_dir = Path(name)
    if project_dir.exists():
        _print(f"[red]Error:[/red] Directory '{name}' already exists")
        raise SystemExit(1)

    project_dir.mkdir(parents=True)
    (project_dir / "app").mkdir()

    # pyproject.toml
    deps = [
        '"neutron>=0.1.0"',
    ]
    if with_ai:
        deps.append('"neutron[ai]>=0.1.0"')

    (project_dir / "pyproject.toml").write_text(textwrap.dedent(f"""\
        [project]
        name = "{name}"
        version = "0.1.0"
        requires-python = ">=3.11"
        dependencies = [
            {", ".join(deps)}
        ]

        [build-system]
        requires = ["hatchling"]
        build-backend = "hatchling.build"
    """))

    # Main app file
    nucleus_import = ""
    nucleus_lifespan = ""
    if with_nucleus:
        nucleus_import = "from neutron.nucleus import NucleusClient\n"
        nucleus_lifespan = textwrap.dedent("""\

            @asynccontextmanager
            async def lifespan(app):
                app.db = await NucleusClient.connect(
                    os.environ.get("DATABASE_URL", "postgresql://localhost:5432/mydb")
                )
                yield
                await app.db.close()
        """)

    ai_code = ""
    if with_ai:
        ai_code = textwrap.dedent("""\

            from neutron.ai import LLM

            llm = LLM(provider="openai")
        """)

    lifespan_arg = ""
    if with_nucleus:
        lifespan_arg = ", lifespan=lifespan"

    (project_dir / "app" / "__init__.py").write_text(textwrap.dedent(f"""\
        \"\"\"Application entry point.\"\"\"

        import os
        from contextlib import asynccontextmanager

        from neutron import App, Router
        {nucleus_import}{ai_code}
        router = Router()
        {nucleus_lifespan}

        @router.get("/")
        async def index() -> dict:
            return {{"message": "Hello from {name}!"}}


        @router.get("/health")
        async def health() -> dict:
            return {{"status": "ok"}}


        app = App(title="{name}"{lifespan_arg})
        app.include_router(router)
    """))

    # .env
    (project_dir / ".env").write_text(textwrap.dedent("""\
        DATABASE_URL=postgresql://localhost:5432/mydb
        NEUTRON_DEBUG=true
    """))

    # .gitignore
    (project_dir / ".gitignore").write_text(textwrap.dedent("""\
        __pycache__/
        *.pyc
        .env
        .venv/
        dist/
    """))

    _print(f"[green]Created project:[/green] {name}/")
    _print(f"  cd {name}")
    _print(f"  pip install -e .")
    _print(f"  python -m neutron dev")


# ---------------------------------------------------------------------------
# neutron dev [app_path]
# ---------------------------------------------------------------------------

@app.command
def dev(
    app_path: Annotated[str, cyclopts.Parameter(help="App import path (module:attribute)")] = "app:app",
    *,
    host: Annotated[str, cyclopts.Parameter(help="Bind host")] = "0.0.0.0",
    port: Annotated[int, cyclopts.Parameter(help="Bind port")] = 8000,
    reload: Annotated[bool, cyclopts.Parameter(help="Enable auto-reload")] = True,
    server: Annotated[str, cyclopts.Parameter(help="Server: uvicorn or granian")] = "uvicorn",
) -> None:
    """Run the development server with auto-reload."""
    if server == "granian":
        try:
            from granian import Granian
            from granian.constants import Interfaces

            g = Granian(
                app_path,
                address=host,
                port=port,
                interface=Interfaces.ASGI,
                reload=reload,
            )
            g.serve()
        except ImportError:
            print("Error: granian not installed. Run: pip install granian")
            raise SystemExit(1)
    else:
        try:
            import uvicorn
        except ImportError:
            print("Error: uvicorn not installed. Run: pip install uvicorn[standard]")
            raise SystemExit(1)

        uvicorn.run(
            app_path,
            host=host,
            port=port,
            reload=reload,
            log_level="info",
        )


# ---------------------------------------------------------------------------
# neutron run [app_path]
# ---------------------------------------------------------------------------

@app.command
def run(
    app_path: Annotated[str, cyclopts.Parameter(help="App import path (module:attribute)")] = "app:app",
    *,
    host: Annotated[str, cyclopts.Parameter(help="Bind host")] = "0.0.0.0",
    port: Annotated[int, cyclopts.Parameter(help="Bind port")] = 8000,
    workers: Annotated[int, cyclopts.Parameter(help="Number of worker processes")] = 1,
    server: Annotated[str, cyclopts.Parameter(help="Server: uvicorn or granian")] = "uvicorn",
) -> None:
    """Run the production server."""
    if server == "granian":
        try:
            from granian import Granian
            from granian.constants import Interfaces

            g = Granian(
                app_path,
                address=host,
                port=port,
                workers=workers,
                interface=Interfaces.ASGI,
            )
            g.serve()
        except ImportError:
            print("Error: granian not installed. Run: pip install granian")
            raise SystemExit(1)
    else:
        try:
            import uvicorn
        except ImportError:
            print("Error: uvicorn not installed. Run: pip install uvicorn[standard]")
            raise SystemExit(1)

        uvicorn.run(
            app_path,
            host=host,
            port=port,
            workers=workers,
            log_level="info",
        )


# ---------------------------------------------------------------------------
# neutron routes <app_path>
# ---------------------------------------------------------------------------

@app.command
def routes(
    app_path: Annotated[str, cyclopts.Parameter(help="App import path (module:attribute)")] = "app:app",
) -> None:
    """List all registered routes."""
    try:
        from rich.console import Console
        from rich.table import Table

        console = Console()
        use_rich = True
    except ImportError:
        use_rich = False

    sys.path.insert(0, os.getcwd())
    module_name, attr_name = app_path.split(":")
    module = importlib.import_module(module_name)
    neutron_app = getattr(module, attr_name)

    # Collect handler info from the app
    handler_info = list(neutron_app.router.get_handler_info())
    for router, prefix in neutron_app._routers:
        handler_info.extend(router.get_handler_info(prefix))

    if use_rich:
        table = Table(title="Routes")
        table.add_column("Method", style="cyan")
        table.add_column("Path", style="green")
        table.add_column("Handler", style="yellow")
        table.add_column("Tags")

        for info in handler_info:
            tags = ", ".join(info.get("tags", []))
            table.add_row(
                info["method"].upper(),
                info["path"],
                info.get("name", ""),
                tags,
            )

        # Add built-in routes
        table.add_row("GET", "/health", "health_endpoint", "built-in")
        table.add_row("GET", "/openapi.json", "openapi_endpoint", "built-in")
        table.add_row("GET", "/docs", "docs_endpoint", "built-in")
        console.print(table)
    else:
        print(f"{'Method':<8} {'Path':<30} {'Handler':<20}")
        print("-" * 60)
        for info in handler_info:
            print(f"{info['method'].upper():<8} {info['path']:<30} {info.get('name', ''):<20}")
        print(f"{'GET':<8} {'/health':<30} {'health_endpoint':<20}")
        print(f"{'GET':<8} {'/openapi.json':<30} {'openapi_endpoint':<20}")
        print(f"{'GET':<8} {'/docs':<30} {'docs_endpoint':<20}")


# ---------------------------------------------------------------------------
# neutron migrate <app_path>
# ---------------------------------------------------------------------------

@app.command
def migrate(
    app_path: Annotated[str, cyclopts.Parameter(help="App import path (module:attribute)")] = "app:app",
    *,
    directory: Annotated[str, cyclopts.Parameter(help="Migrations directory")] = "migrations",
) -> None:
    """Run database migrations."""
    import asyncio

    sys.path.insert(0, os.getcwd())
    module_name, attr_name = app_path.split(":")
    module = importlib.import_module(module_name)
    neutron_app = getattr(module, attr_name)

    async def _run() -> None:
        db = neutron_app.db
        if db is None:
            print("Error: No database connection. Ensure app.db is set in lifespan.")
            raise SystemExit(1)

        migrations_dir = Path(directory)
        if not migrations_dir.exists():
            print(f"Error: Migrations directory '{directory}' not found")
            raise SystemExit(1)

        sql_files = sorted(migrations_dir.glob("*.sql"))
        if not sql_files:
            print("No migration files found.")
            return

        applied = await db.migration_status()
        for sql_file in sql_files:
            name = sql_file.name
            if name in applied:
                print(f"  skip  {name}")
                continue
            sql = sql_file.read_text()
            await db.migrate(name, sql)
            print(f"  apply {name}")

        print("Migrations complete.")

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """CLI entry point."""
    app()
