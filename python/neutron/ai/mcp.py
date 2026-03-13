"""MCP (Model Context Protocol) — server and client."""

from __future__ import annotations

import json
import re
from typing import Any, Callable

import httpx
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from neutron.ai.tools import Tool, _build_parameters_schema


# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------


class MCPServer:
    """MCP-compatible tool + resource server.

    Exposes tools and resources via a JSON-RPC-style HTTP interface
    that can be consumed by MCP clients (Claude Desktop, other agents).

    Usage::

        mcp = MCPServer(name="my-tools")

        @mcp.tool()
        async def search(query: str) -> list[dict]:
            \"\"\"Search the database.\"\"\"
            ...

        @mcp.resource("users://{user_id}")
        async def get_user(user_id: str) -> dict:
            \"\"\"Get user by ID.\"\"\"
            ...

        app.mount("/mcp", mcp)
    """

    def __init__(self, name: str = "neutron-mcp", version: str = "1.0.0") -> None:
        self.name = name
        self.version = version
        self._tools: dict[str, Tool] = {}
        self._resources: dict[str, _ResourceDef] = {}

    def tool(self, name: str | None = None) -> Callable:
        """Decorator to register a function as an MCP tool."""

        def decorator(fn: Callable) -> Callable:
            tool_name = name or fn.__name__
            t = Tool(fn, name=tool_name)
            self._tools[tool_name] = t
            return fn

        return decorator

    def resource(self, uri_template: str) -> Callable:
        """Decorator to register a resource with a URI template.

        URI templates use ``{param}`` placeholders::

            @mcp.resource("users://{user_id}")
            async def get_user(user_id: str) -> dict: ...
        """

        def decorator(fn: Callable) -> Callable:
            res = _ResourceDef(uri_template, fn)
            self._resources[uri_template] = res
            return fn

        return decorator

    # --- ASGI / Starlette integration ---

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        """ASGI interface — route MCP requests."""
        from starlette.applications import Starlette
        from starlette.routing import Route

        routes = [
            Route("/", self._handle_root, methods=["GET"]),
            Route("/tools", self._handle_list_tools, methods=["GET"]),
            Route("/tools/{tool_name}", self._handle_call_tool, methods=["POST"]),
            Route("/resources", self._handle_list_resources, methods=["GET"]),
            Route(
                "/resources/{resource_path:path}",
                self._handle_read_resource,
                methods=["GET"],
            ),
        ]
        app = Starlette(routes=routes)
        await app(scope, receive, send)

    async def _handle_root(self, request: Request) -> JSONResponse:
        """Server info / capabilities."""
        return JSONResponse(
            {
                "name": self.name,
                "version": self.version,
                "protocol": "mcp",
                "capabilities": {
                    "tools": len(self._tools) > 0,
                    "resources": len(self._resources) > 0,
                },
            }
        )

    async def _handle_list_tools(self, request: Request) -> JSONResponse:
        """List available tools with their schemas."""
        tools = []
        for t in self._tools.values():
            tools.append(
                {
                    "name": t.name,
                    "description": t.description,
                    "inputSchema": t.schema,
                }
            )
        return JSONResponse({"tools": tools})

    async def _handle_call_tool(self, request: Request) -> JSONResponse:
        """Call a tool by name."""
        tool_name = request.path_params["tool_name"]
        tool_obj = self._tools.get(tool_name)
        if tool_obj is None:
            return JSONResponse(
                {"error": f"Unknown tool: {tool_name}"}, status_code=404
            )

        try:
            body = await request.json()
        except Exception:
            body = {}

        try:
            result = await tool_obj(**body)
            return JSONResponse({"result": result})
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)

    async def _handle_list_resources(self, request: Request) -> JSONResponse:
        """List available resources."""
        resources = []
        for uri_template, res in self._resources.items():
            resources.append(
                {
                    "uri_template": uri_template,
                    "name": res.fn.__name__,
                    "description": (res.fn.__doc__ or "").strip(),
                }
            )
        return JSONResponse({"resources": resources})

    async def _handle_read_resource(self, request: Request) -> JSONResponse:
        """Read a resource by URI."""
        resource_path = request.path_params["resource_path"]

        for uri_template, res in self._resources.items():
            params = res.match(resource_path)
            if params is not None:
                try:
                    result = await res.call(**params)
                    return JSONResponse({"result": result})
                except Exception as e:
                    return JSONResponse({"error": str(e)}, status_code=500)

        return JSONResponse(
            {"error": f"No resource matches: {resource_path}"}, status_code=404
        )


# ---------------------------------------------------------------------------
# Resource definition
# ---------------------------------------------------------------------------


class _ResourceDef:
    """A resource with a URI template and handler function."""

    def __init__(self, uri_template: str, fn: Callable) -> None:
        self.uri_template = uri_template
        self.fn = fn
        self._is_async = _is_coroutine(fn)

        # Build regex from URI template: "users://{user_id}" → "users://(?P<user_id>[^/]+)"
        pattern = re.sub(r"\{(\w+)\}", r"(?P<\1>[^/]+)", uri_template)
        self._pattern = re.compile(f"^{pattern}$")

    def match(self, uri: str) -> dict[str, str] | None:
        """Try to match a URI against this template. Returns params or None."""
        m = self._pattern.match(uri)
        if m:
            return m.groupdict()
        return None

    async def call(self, **kwargs: Any) -> Any:
        if self._is_async:
            return await self.fn(**kwargs)
        return self.fn(**kwargs)


def _is_coroutine(fn: Callable) -> bool:
    import inspect

    return inspect.iscoroutinefunction(fn)


# ---------------------------------------------------------------------------
# MCP Client
# ---------------------------------------------------------------------------


class MCPClient:
    """Client for connecting to MCP servers.

    Usage::

        async with MCPClient("http://other-service/mcp") as client:
            tools = await client.list_tools()
            result = await client.call_tool("search", {"query": "hello"})
    """

    def __init__(self, url: str) -> None:
        self.url = url.rstrip("/")
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> MCPClient:
        self._client = httpx.AsyncClient(timeout=60.0)
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("MCPClient must be used as an async context manager")
        return self._client

    async def server_info(self) -> dict[str, Any]:
        """Get server info and capabilities."""
        client = self._ensure_client()
        resp = await client.get(f"{self.url}/")
        resp.raise_for_status()
        return resp.json()

    async def list_tools(self) -> list[dict[str, Any]]:
        """List available tools on the server."""
        client = self._ensure_client()
        resp = await client.get(f"{self.url}/tools")
        resp.raise_for_status()
        return resp.json().get("tools", [])

    async def call_tool(
        self, name: str, arguments: dict[str, Any] | None = None
    ) -> Any:
        """Call a tool on the MCP server."""
        client = self._ensure_client()
        resp = await client.post(
            f"{self.url}/tools/{name}",
            json=arguments or {},
        )
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"MCP tool error: {data['error']}")
        return data.get("result")

    async def list_resources(self) -> list[dict[str, Any]]:
        """List available resources on the server."""
        client = self._ensure_client()
        resp = await client.get(f"{self.url}/resources")
        resp.raise_for_status()
        return resp.json().get("resources", [])

    async def read_resource(self, uri: str) -> Any:
        """Read a resource by URI."""
        client = self._ensure_client()
        resp = await client.get(f"{self.url}/resources/{uri}")
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"MCP resource error: {data['error']}")
        return data.get("result")
