"""AI integration for neutron_sim: MCP tools and surrogate models."""

from .mcp_tool import mcp_tool, list_tools, get_tool, call_tool
from .surrogate import SurrogateModel, train_surrogate, train_surrogate_from_nucleus

__all__ = [
    "mcp_tool",
    "list_tools",
    "get_tool",
    "call_tool",
    "SurrogateModel",
    "train_surrogate",
    "train_surrogate_from_nucleus",
]
