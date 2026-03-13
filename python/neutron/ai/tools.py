"""Tool definition and registry — auto-generates JSON Schema from function signatures."""

from __future__ import annotations

import inspect
import json
from typing import Any, Callable, get_type_hints


class Tool:
    """A wrapped function that can be called by an LLM."""

    def __init__(self, fn: Callable, name: str | None = None) -> None:
        self.fn = fn
        self.name = name or fn.__name__
        self.description = (fn.__doc__ or "").strip()
        self.is_async = inspect.iscoroutinefunction(fn)
        self.schema = _build_parameters_schema(fn)

    async def __call__(self, **kwargs: Any) -> Any:
        if self.is_async:
            return await self.fn(**kwargs)
        return self.fn(**kwargs)

    def to_openai_schema(self) -> dict[str, Any]:
        """Convert to OpenAI function-calling tool schema."""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.schema,
            },
        }

    def __repr__(self) -> str:
        return f"Tool({self.name})"


def tool(fn: Callable | None = None, *, name: str | None = None) -> Tool | Callable:
    """Decorator to register a function as an LLM tool.

    Usage::

        @tool
        async def get_weather(city: str, units: str = "celsius") -> dict:
            \"\"\"Get current weather for a city.\"\"\"
            ...

        # Or with custom name:
        @tool(name="weather")
        async def get_weather(city: str) -> dict: ...
    """
    if fn is not None:
        return Tool(fn, name=name)

    def decorator(f: Callable) -> Tool:
        return Tool(f, name=name)

    return decorator


def tools_to_openai_schema(tools: list[Tool | Callable]) -> list[dict[str, Any]]:
    """Convert a list of tools to OpenAI function-calling schema."""
    result = []
    for t in tools:
        if isinstance(t, Tool):
            result.append(t.to_openai_schema())
        elif callable(t):
            result.append(Tool(t).to_openai_schema())
    return result


def resolve_tool_call(
    tools: list[Tool | Callable], name: str, arguments: dict[str, Any]
) -> Tool | None:
    """Find a tool by name from a list."""
    for t in tools:
        tool_obj = t if isinstance(t, Tool) else Tool(t)
        if tool_obj.name == name:
            return tool_obj
    return None


# ---------------------------------------------------------------------------
# Schema generation from function signatures
# ---------------------------------------------------------------------------

_TYPE_MAP: dict[type, str] = {
    str: "string",
    int: "integer",
    float: "number",
    bool: "boolean",
}


def _build_parameters_schema(fn: Callable) -> dict[str, Any]:
    """Build a JSON Schema for a function's parameters."""
    sig = inspect.signature(fn)
    try:
        hints = get_type_hints(fn)
    except Exception:
        hints = {}

    properties: dict[str, Any] = {}
    required: list[str] = []

    for pname, param in sig.parameters.items():
        if pname == "self":
            continue
        annotation = hints.get(pname, str)
        prop = _type_to_json_schema(annotation)
        properties[pname] = prop

        if param.default is inspect.Parameter.empty:
            required.append(pname)
        else:
            prop["default"] = param.default

    schema: dict[str, Any] = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


def _type_to_json_schema(t: type[Any]) -> dict[str, Any]:
    """Convert a Python type annotation to JSON Schema."""
    if t in _TYPE_MAP:
        return {"type": _TYPE_MAP[t]}

    origin = getattr(t, "__origin__", None)
    if origin is list:
        args = getattr(t, "__args__", ())
        items = _type_to_json_schema(args[0]) if args else {"type": "string"}
        return {"type": "array", "items": items}

    if origin is dict:
        return {"type": "object"}

    # Pydantic model
    from pydantic import BaseModel

    if isinstance(t, type) and issubclass(t, BaseModel):
        return t.model_json_schema()

    return {"type": "string"}
