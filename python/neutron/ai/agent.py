"""Agent framework — tool loops, multi-turn, and handoffs."""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel

from neutron.ai.providers import LLM, LLMResponse, Message
from neutron.ai.tools import Tool, resolve_tool_call, tools_to_openai_schema


class AgentResult(BaseModel):
    content: str
    messages: list[dict[str, Any]] = []
    tool_calls_made: int = 0


class Agent:
    """Base agent with tool use and multi-turn conversation.

    Subclass and add tools via the ``@tool`` decorator::

        class MyAgent(Agent):
            llm = LLM(provider="openai", model="gpt-4o")
            system_prompt = "You are a helpful assistant."

            @tool
            async def search(self, query: str) -> str:
                \"\"\"Search the knowledge base.\"\"\"
                ...

        agent = MyAgent()
        result = await agent.run("Find info about quantum computing")
    """

    llm: LLM | None = None
    system_prompt: str = "You are a helpful assistant."
    max_turns: int = 20

    def __init__(
        self,
        llm: LLM | None = None,
        db: Any = None,
        **kwargs: Any,
    ) -> None:
        if llm is not None:
            self.llm = llm
        self.db = db
        for k, v in kwargs.items():
            setattr(self, k, v)

        # Collect tools from class methods decorated with @tool
        self._tools: list[Tool] = []
        for attr_name in dir(self.__class__):
            attr = getattr(self.__class__, attr_name, None)
            if isinstance(attr, Tool):
                # Bind self to the tool function
                _fn = attr.fn
                _self = self
                bound_fn = lambda _fn=_fn, _self=_self, **kw: _fn(_self, **kw)
                bound_tool = Tool(bound_fn, name=attr.name)
                bound_tool.description = attr.description
                bound_tool.schema = attr.schema
                bound_tool.is_async = attr.is_async
                self._tools.append(bound_tool)

        # Also collect handoff tools
        for attr_name in dir(self):
            attr = getattr(self, attr_name, None)
            if isinstance(attr, _HandoffTool):
                self._tools.append(attr.as_tool())

    async def run(
        self,
        prompt: str,
        *,
        messages: list[Message] | None = None,
        tools: list[Tool] | None = None,
    ) -> AgentResult:
        """Run the agent loop until the LLM produces a final response."""
        if self.llm is None:
            raise ValueError("Agent requires an LLM. Set llm class attribute or pass to __init__.")

        all_tools = list(self._tools)
        if tools:
            all_tools.extend(tools)

        conversation: list[Message] = list(messages or [])
        if self.system_prompt:
            conversation.insert(0, {"role": "system", "content": self.system_prompt})
        conversation.append({"role": "user", "content": prompt})

        tool_defs = tools_to_openai_schema(all_tools) if all_tools else None
        total_tool_calls = 0

        for _turn in range(self.max_turns):
            response = await self.llm._provider.chat(
                conversation,
                tools=tool_defs,
                temperature=self.llm.temperature,
                max_tokens=self.llm.max_tokens,
            )

            if not response.tool_calls:
                # Final response — no more tool calls
                conversation.append(
                    {"role": "assistant", "content": response.content}
                )
                return AgentResult(
                    content=response.content,
                    messages=conversation,
                    tool_calls_made=total_tool_calls,
                )

            # Process tool calls
            # Add assistant message with tool calls
            conversation.append(
                {
                    "role": "assistant",
                    "content": response.content or "",
                    "tool_calls": [
                        {
                            "id": tc["id"],
                            "type": "function",
                            "function": {
                                "name": tc["name"],
                                "arguments": json.dumps(tc["arguments"]),
                            },
                        }
                        for tc in response.tool_calls
                    ],
                }
            )

            for tc in response.tool_calls:
                total_tool_calls += 1
                tool_obj = resolve_tool_call(all_tools, tc["name"], tc["arguments"])
                if tool_obj is None:
                    tool_result = f"Error: Unknown tool '{tc['name']}'"
                else:
                    try:
                        result = await tool_obj(**tc["arguments"])
                        tool_result = (
                            json.dumps(result) if not isinstance(result, str) else result
                        )
                    except Exception as e:
                        tool_result = f"Error: {e}"

                conversation.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc["id"],
                        "content": tool_result,
                    }
                )

        # Max turns reached
        return AgentResult(
            content="Max turns reached without a final response.",
            messages=conversation,
            tool_calls_made=total_tool_calls,
        )


# ---------------------------------------------------------------------------
# Handoffs
# ---------------------------------------------------------------------------


class _HandoffTool:
    """Marker for agent handoff methods."""

    def __init__(self, target_cls: type[Any], fn: Any) -> None:
        self.target_cls = target_cls
        self.fn = fn
        self.name = fn.__name__
        self.description = (fn.__doc__ or "").strip()

    def as_tool(self) -> Tool:
        from neutron.ai.tools import _build_parameters_schema

        async def handoff_fn(**kwargs: Any) -> str:
            target = self.target_cls()
            # The first arg is typically the prompt/topic
            prompt = " ".join(str(v) for v in kwargs.values())
            result = await target.run(prompt)
            return result.content

        t = Tool(handoff_fn, name=self.name)
        t.description = self.description or f"Hand off to {self.target_cls.__name__}"
        t.schema = _build_parameters_schema(self.fn)
        return t


def handoff(to: type[Any]) -> Any:
    """Decorator to create a handoff tool to another agent.

    Usage::

        class TriageAgent(Agent):
            @handoff(to=ResearchAgent)
            async def research(self, topic: str) -> str:
                \"\"\"Hand off to research specialist.\"\"\"
    """

    def decorator(fn: Any) -> _HandoffTool:
        return _HandoffTool(to, fn)

    return decorator
