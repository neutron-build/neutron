"""MCP tool registration for neutron_sim simulations.

Exposes simulation capabilities as MCP tools so Neutron Python AI agents can
trigger simulations with natural language:  "simulate this spring-mass system".
"""

from __future__ import annotations
import functools
import inspect
from typing import Any, Callable

# Registry: tool_name → (function, schema)
_TOOLS: dict[str, dict] = {}


def mcp_tool(name: str | None = None, description: str | None = None):
    """Decorator that registers a function as an MCP simulation tool.

    Usage
    -----
    ::

        @mcp_tool("simulate")
        async def simulate_system(description: str, parameters: dict) -> dict:
            ...

    The decorated function is still callable normally; registration only
    adds it to the global tool registry.
    """

    def decorator(fn: Callable) -> Callable:
        tool_name = name or fn.__name__
        tool_desc = description or (inspect.getdoc(fn) or "")

        # Build a simple JSON-schema from type hints
        hints = {}
        try:
            hints = fn.__annotations__
        except AttributeError:
            pass

        schema: dict[str, Any] = {
            "name": tool_name,
            "description": tool_desc,
            "function": fn,
            "parameters": {
                k: {"type": _py_type_to_json(v)}
                for k, v in hints.items()
                if k != "return"
            },
        }
        _TOOLS[tool_name] = schema

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        wrapper._mcp_tool_name = tool_name  # type: ignore[attr-defined]
        return wrapper

    return decorator


def _py_type_to_json(t) -> str:
    mapping = {str: "string", int: "integer", float: "number", bool: "boolean", dict: "object", list: "array"}
    return mapping.get(t, "string")


def list_tools() -> list[dict]:
    """Return all registered MCP tool schemas."""
    return [
        {k: v for k, v in schema.items() if k != "function"}
        for schema in _TOOLS.values()
    ]


def get_tool(name: str) -> Callable | None:
    """Return the callable for a registered tool, or None."""
    schema = _TOOLS.get(name)
    return schema["function"] if schema else None


def call_tool(name: str, **kwargs):
    """Call a registered tool by name with keyword arguments."""
    fn = get_tool(name)
    if fn is None:
        raise KeyError(f"MCP tool '{name}' not registered.")
    return fn(**kwargs)
