"""LLM provider abstraction — OpenAI, Anthropic, and local/Ollama."""

from __future__ import annotations

import inspect
import json
import os
from collections.abc import AsyncIterator
from typing import Any, TypeVar

import httpx
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)

# ---------------------------------------------------------------------------
# Message types
# ---------------------------------------------------------------------------

Message = dict[str, Any]  # {"role": "...", "content": "..."}


class LLMResponse(BaseModel):
    content: str
    role: str = "assistant"
    tool_calls: list[dict[str, Any]] = []
    usage: dict[str, int] = {}
    raw: dict[str, Any] = {}


# ---------------------------------------------------------------------------
# Provider base
# ---------------------------------------------------------------------------


class _Provider:
    """Abstract provider interface."""

    async def chat(
        self,
        messages: list[Message],
        *,
        tools: list[dict[str, Any]] | None = None,
        response_format: dict[str, Any] | None = None,
        temperature: float = 0.7,
        max_tokens: int = 4096,
    ) -> LLMResponse:
        raise NotImplementedError

    async def stream(
        self,
        messages: list[Message],
        *,
        temperature: float = 0.7,
        max_tokens: int = 4096,
    ) -> AsyncIterator[str]:
        raise NotImplementedError
        yield  # pragma: no cover

    async def embed(self, texts: list[str]) -> list[list[float]]:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# OpenAI-compatible provider
# ---------------------------------------------------------------------------


class _OpenAIProvider(_Provider):
    def __init__(
        self,
        model: str,
        api_key: str,
        base_url: str = "https://api.openai.com/v1",
    ) -> None:
        self.model = model
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")

    async def chat(
        self,
        messages: list[Message],
        *,
        tools: list[dict[str, Any]] | None = None,
        response_format: dict[str, Any] | None = None,
        temperature: float = 0.7,
        max_tokens: int = 4096,
    ) -> LLMResponse:
        body: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if tools:
            body["tools"] = tools
        if response_format:
            body["response_format"] = response_format

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self._base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": "application/json",
                },
                json=body,
                timeout=120.0,
            )
            resp.raise_for_status()
            data = resp.json()

        choice = data["choices"][0]
        msg = choice["message"]
        tool_calls = []
        for tc in msg.get("tool_calls", []):
            tool_calls.append(
                {
                    "id": tc["id"],
                    "name": tc["function"]["name"],
                    "arguments": json.loads(tc["function"]["arguments"]),
                }
            )

        return LLMResponse(
            content=msg.get("content") or "",
            role=msg.get("role", "assistant"),
            tool_calls=tool_calls,
            usage=data.get("usage", {}),
            raw=data,
        )

    async def stream(
        self,
        messages: list[Message],
        *,
        temperature: float = 0.7,
        max_tokens: int = 4096,
    ) -> AsyncIterator[str]:
        body: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": True,
        }
        async with httpx.AsyncClient() as client:
            async with client.stream(
                "POST",
                f"{self._base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": "application/json",
                },
                json=body,
                timeout=120.0,
            ) as resp:
                resp.raise_for_status()
                async for line in resp.aiter_lines():
                    if not line.startswith("data: "):
                        continue
                    payload = line[6:]
                    if payload.strip() == "[DONE]":
                        break
                    chunk = json.loads(payload)
                    delta = chunk["choices"][0].get("delta", {})
                    if "content" in delta and delta["content"]:
                        yield delta["content"]

    async def embed(self, texts: list[str]) -> list[list[float]]:
        body = {"model": self.model, "input": texts}
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self._base_url}/embeddings",
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": "application/json",
                },
                json=body,
                timeout=60.0,
            )
            resp.raise_for_status()
            data = resp.json()
        return [item["embedding"] for item in data["data"]]


# ---------------------------------------------------------------------------
# Anthropic provider
# ---------------------------------------------------------------------------


class _AnthropicProvider(_Provider):
    def __init__(self, model: str, api_key: str) -> None:
        self.model = model
        self._api_key = api_key

    async def chat(
        self,
        messages: list[Message],
        *,
        tools: list[dict[str, Any]] | None = None,
        response_format: dict[str, Any] | None = None,
        temperature: float = 0.7,
        max_tokens: int = 4096,
    ) -> LLMResponse:
        # Extract system message
        system = ""
        chat_msgs: list[dict[str, Any]] = []
        for m in messages:
            if m["role"] == "system":
                system = m["content"]
            else:
                chat_msgs.append(m)

        body: dict[str, Any] = {
            "model": self.model,
            "messages": chat_msgs,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        if system:
            body["system"] = system
        if tools:
            # Convert OpenAI-style tools to Anthropic format
            body["tools"] = [
                {
                    "name": t["function"]["name"],
                    "description": t["function"].get("description", ""),
                    "input_schema": t["function"].get("parameters", {}),
                }
                for t in tools
            ]

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": self._api_key,
                    "anthropic-version": "2023-06-01",
                    "Content-Type": "application/json",
                },
                json=body,
                timeout=120.0,
            )
            resp.raise_for_status()
            data = resp.json()

        content = ""
        tool_calls = []
        for block in data.get("content", []):
            if block["type"] == "text":
                content += block["text"]
            elif block["type"] == "tool_use":
                tool_calls.append(
                    {
                        "id": block["id"],
                        "name": block["name"],
                        "arguments": block["input"],
                    }
                )

        return LLMResponse(
            content=content,
            role="assistant",
            tool_calls=tool_calls,
            usage=data.get("usage", {}),
            raw=data,
        )

    async def stream(
        self,
        messages: list[Message],
        *,
        temperature: float = 0.7,
        max_tokens: int = 4096,
    ) -> AsyncIterator[str]:
        system = ""
        chat_msgs: list[dict[str, Any]] = []
        for m in messages:
            if m["role"] == "system":
                system = m["content"]
            else:
                chat_msgs.append(m)

        body: dict[str, Any] = {
            "model": self.model,
            "messages": chat_msgs,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "stream": True,
        }
        if system:
            body["system"] = system

        async with httpx.AsyncClient() as client:
            async with client.stream(
                "POST",
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": self._api_key,
                    "anthropic-version": "2023-06-01",
                    "Content-Type": "application/json",
                },
                json=body,
                timeout=120.0,
            ) as resp:
                resp.raise_for_status()
                async for line in resp.aiter_lines():
                    if not line.startswith("data: "):
                        continue
                    event = json.loads(line[6:])
                    if event.get("type") == "content_block_delta":
                        delta = event.get("delta", {})
                        if delta.get("type") == "text_delta":
                            yield delta["text"]


# ---------------------------------------------------------------------------
# LLM — the public interface
# ---------------------------------------------------------------------------

_PROVIDERS = {
    "openai": _OpenAIProvider,
    "anthropic": _AnthropicProvider,
    "local": _OpenAIProvider,  # Ollama/vLLM use OpenAI-compatible API
}


class LLM:
    """Provider-agnostic LLM interface.

    Usage::

        llm = LLM(provider="openai", model="gpt-4o")
        response = await llm.complete("Hello!")

        result = await llm.extract(MyModel, "Extract entities from: ...")

        async for chunk in llm.stream("Tell me a story"):
            print(chunk, end="")
    """

    def __init__(
        self,
        provider: str = "openai",
        model: str = "gpt-4o",
        api_key: str | None = None,
        base_url: str | None = None,
        temperature: float = 0.7,
        max_tokens: int = 4096,
    ) -> None:
        self.provider_name = provider
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens

        resolved_key = api_key or os.environ.get("NEUTRON_AI_API_KEY", "")
        if not resolved_key:
            # Try provider-specific env vars
            if provider == "openai":
                resolved_key = os.environ.get("OPENAI_API_KEY", "")
            elif provider == "anthropic":
                resolved_key = os.environ.get("ANTHROPIC_API_KEY", "")

        kwargs: dict[str, Any] = {"model": model, "api_key": resolved_key}
        if base_url:
            kwargs["base_url"] = base_url
        elif provider == "local":
            kwargs["base_url"] = os.environ.get(
                "NEUTRON_AI_BASE_URL", "http://localhost:11434/v1"
            )

        provider_cls = _PROVIDERS.get(provider)
        if provider_cls is None:
            raise ValueError(
                f"Unknown provider '{provider}'. "
                f"Available: {', '.join(_PROVIDERS)}"
            )
        self._provider: _Provider = provider_cls(**kwargs)

    # --- High-level API ---

    async def complete(self, prompt: str, **kwargs: Any) -> str:
        """Simple text completion."""
        resp = await self._provider.chat(
            [{"role": "user", "content": prompt}],
            temperature=kwargs.pop("temperature", self.temperature),
            max_tokens=kwargs.pop("max_tokens", self.max_tokens),
            **kwargs,
        )
        return resp.content

    async def chat(
        self,
        messages: list[Message],
        *,
        tools: list[Any] | None = None,
        **kwargs: Any,
    ) -> LLMResponse:
        """Chat completion with optional tool use."""
        tool_defs = None
        if tools:
            from neutron.ai.tools import tools_to_openai_schema

            tool_defs = tools_to_openai_schema(tools)

        return await self._provider.chat(
            messages,
            tools=tool_defs,
            temperature=kwargs.pop("temperature", self.temperature),
            max_tokens=kwargs.pop("max_tokens", self.max_tokens),
            **kwargs,
        )

    async def extract(
        self,
        model: type[T],
        prompt: str,
        *,
        max_retries: int = 3,
        **kwargs: Any,
    ) -> T:
        """Extract structured data into a Pydantic model (Instructor pattern)."""
        from neutron.ai.structured import extract_structured

        return await extract_structured(
            self, model, prompt, max_retries=max_retries, **kwargs
        )

    async def stream(self, prompt: str, **kwargs: Any) -> AsyncIterator[str]:
        """Stream a completion token by token."""
        async for chunk in self._provider.stream(
            [{"role": "user", "content": prompt}],
            temperature=kwargs.pop("temperature", self.temperature),
            max_tokens=kwargs.pop("max_tokens", self.max_tokens),
        ):
            yield chunk

    async def embed(self, texts: list[str] | str) -> list[list[float]]:
        """Generate embeddings."""
        if isinstance(texts, str):
            texts = [texts]
        return await self._provider.embed(texts)
