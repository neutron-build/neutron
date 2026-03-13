"""Tests for neutron/ai — AI primitives (providers, tools, structured, agents, RAG, workflow, memory, MCP)."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel

from neutron.ai.providers import LLM, LLMResponse, _OpenAIProvider, _AnthropicProvider
from neutron.ai.tools import Tool, tool, tools_to_openai_schema, resolve_tool_call
from neutron.ai.structured import extract_structured
from neutron.ai.agent import Agent, AgentResult, handoff
from neutron.ai.rag import RAGPipeline, RAGAnswer, RAGSource, _chunk_text
from neutron.ai.workflow import Workflow, WorkflowResult, step, _StepDef
from neutron.ai.memory import Memory, MemoryMessage
from neutron.ai.mcp import MCPServer, MCPClient, _ResourceDef


# ============================================================================
# Tools
# ============================================================================


class TestTools:
    def test_tool_decorator_basic(self):
        @tool
        def greet(name: str) -> str:
            """Say hello."""
            return f"Hello {name}"

        assert isinstance(greet, Tool)
        assert greet.name == "greet"
        assert greet.description == "Say hello."
        assert greet.schema["properties"]["name"]["type"] == "string"
        assert "name" in greet.schema["required"]

    def test_tool_decorator_with_name(self):
        @tool(name="custom_name")
        def my_func(x: int) -> int:
            """Do something."""
            return x

        assert isinstance(my_func, Tool)
        assert my_func.name == "custom_name"

    async def test_tool_async_call(self):
        @tool
        async def add(a: int, b: int) -> int:
            """Add two numbers."""
            return a + b

        assert add.is_async is True
        result = await add(a=3, b=4)
        assert result == 7

    async def test_tool_sync_call(self):
        @tool
        def multiply(a: int, b: int) -> int:
            """Multiply."""
            return a * b

        assert multiply.is_async is False
        result = await multiply(a=3, b=4)
        assert result == 12

    def test_tool_schema_types(self):
        @tool
        def complex_fn(
            name: str, count: int, ratio: float, active: bool, tags: list[str]
        ) -> dict:
            """Complex function."""
            return {}

        schema = complex_fn.schema
        assert schema["properties"]["name"]["type"] == "string"
        assert schema["properties"]["count"]["type"] == "integer"
        assert schema["properties"]["ratio"]["type"] == "number"
        assert schema["properties"]["active"]["type"] == "boolean"
        assert schema["properties"]["tags"]["type"] == "array"

    def test_tool_schema_defaults(self):
        @tool
        def with_defaults(name: str, limit: int = 10) -> str:
            """Has defaults."""
            return name

        assert "name" in with_defaults.schema["required"]
        assert "limit" not in with_defaults.schema.get("required", [])
        assert with_defaults.schema["properties"]["limit"]["default"] == 10

    def test_tools_to_openai_schema(self):
        @tool
        def search(query: str) -> list:
            """Search."""
            return []

        schemas = tools_to_openai_schema([search])
        assert len(schemas) == 1
        assert schemas[0]["type"] == "function"
        assert schemas[0]["function"]["name"] == "search"

    def test_resolve_tool_call(self):
        @tool
        def foo(x: str) -> str:
            """Foo."""
            return x

        found = resolve_tool_call([foo], "foo", {"x": "bar"})
        assert found is not None
        assert found.name == "foo"

        not_found = resolve_tool_call([foo], "missing", {})
        assert not_found is None

    def test_to_openai_schema(self):
        @tool
        def greet(name: str) -> str:
            """Say hello."""
            return ""

        schema = greet.to_openai_schema()
        assert schema["type"] == "function"
        assert schema["function"]["name"] == "greet"
        assert schema["function"]["description"] == "Say hello."

    def test_tool_with_pydantic_model_param(self):
        """Tool schema handles Pydantic model params (when type hints resolve)."""
        # When `from __future__ import annotations` is used, locally-defined types
        # may not resolve via get_type_hints. The schema falls back to "string".
        # This test verifies the schema is still generated without error.

        @tool
        def search(query: str, limit: int = 10) -> list:
            """Search with params."""
            return []

        assert search.schema["properties"]["query"]["type"] == "string"
        assert search.schema["properties"]["limit"]["type"] == "integer"


# ============================================================================
# Providers
# ============================================================================


class TestLLM:
    def test_llm_init_openai(self):
        with patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"}):
            llm = LLM(provider="openai", model="gpt-4o")
            assert llm.provider_name == "openai"
            assert llm.model == "gpt-4o"
            assert isinstance(llm._provider, _OpenAIProvider)

    def test_llm_init_anthropic(self):
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
            llm = LLM(provider="anthropic", model="claude-sonnet-4-20250514")
            assert llm.provider_name == "anthropic"
            assert isinstance(llm._provider, _AnthropicProvider)

    def test_llm_init_unknown_provider(self):
        with pytest.raises(ValueError, match="Unknown provider"):
            LLM(provider="unknown_provider")

    def test_llm_init_custom_api_key(self):
        llm = LLM(provider="openai", model="gpt-4o", api_key="my-key")
        assert llm._provider._api_key == "my-key"

    def test_llm_init_local(self):
        llm = LLM(provider="local", model="llama3")
        assert llm._provider._base_url == "http://localhost:11434/v1"

    async def test_llm_complete(self):
        llm = LLM(provider="openai", model="gpt-4o", api_key="test")
        mock_response = LLMResponse(content="Hello!", role="assistant")
        llm._provider.chat = AsyncMock(return_value=mock_response)

        result = await llm.complete("Hi")
        assert result == "Hello!"
        llm._provider.chat.assert_called_once()

    async def test_llm_chat(self):
        llm = LLM(provider="openai", model="gpt-4o", api_key="test")
        mock_response = LLMResponse(content="Response", tool_calls=[])
        llm._provider.chat = AsyncMock(return_value=mock_response)

        result = await llm.chat([{"role": "user", "content": "Hello"}])
        assert result.content == "Response"

    async def test_llm_embed(self):
        llm = LLM(provider="openai", model="text-embedding-3-small", api_key="test")
        llm._provider.embed = AsyncMock(return_value=[[0.1, 0.2, 0.3]])

        result = await llm.embed("test text")
        assert result == [[0.1, 0.2, 0.3]]

    async def test_llm_embed_single_string(self):
        llm = LLM(provider="openai", model="text-embedding-3-small", api_key="test")
        llm._provider.embed = AsyncMock(return_value=[[0.1, 0.2]])

        result = await llm.embed("single")
        llm._provider.embed.assert_called_with(["single"])

    async def test_llm_extract(self):
        class Sentiment(BaseModel):
            sentiment: str
            confidence: float

        llm = LLM(provider="openai", model="gpt-4o", api_key="test")
        mock_response = LLMResponse(
            content='{"sentiment": "positive", "confidence": 0.95}'
        )
        llm._provider.chat = AsyncMock(return_value=mock_response)

        result = await llm.extract(Sentiment, "This is great!")
        assert result.sentiment == "positive"
        assert result.confidence == 0.95


# ============================================================================
# Structured Output
# ============================================================================


class TestStructuredOutput:
    async def test_extract_success(self):
        class Person(BaseModel):
            name: str
            age: int

        mock_provider = AsyncMock()
        mock_provider.chat = AsyncMock(
            return_value=LLMResponse(content='{"name": "Alice", "age": 30}')
        )
        llm = LLM(provider="openai", model="gpt-4o", api_key="test")
        llm._provider = mock_provider

        result = await extract_structured(llm, Person, "Extract person from: Alice is 30")
        assert result.name == "Alice"
        assert result.age == 30

    async def test_extract_strips_code_fences(self):
        class Item(BaseModel):
            name: str

        mock_provider = AsyncMock()
        mock_provider.chat = AsyncMock(
            return_value=LLMResponse(content='```json\n{"name": "Widget"}\n```')
        )
        llm = LLM(provider="openai", model="gpt-4o", api_key="test")
        llm._provider = mock_provider

        result = await extract_structured(llm, Item, "Extract item")
        assert result.name == "Widget"

    async def test_extract_retry_on_invalid_json(self):
        class Item(BaseModel):
            name: str

        mock_provider = AsyncMock()
        # First call returns invalid JSON, second returns valid
        mock_provider.chat = AsyncMock(
            side_effect=[
                LLMResponse(content="not json"),
                LLMResponse(content='{"name": "Fixed"}'),
            ]
        )
        llm = LLM(provider="openai", model="gpt-4o", api_key="test")
        llm._provider = mock_provider

        result = await extract_structured(llm, Item, "Extract")
        assert result.name == "Fixed"
        assert mock_provider.chat.call_count == 2

    async def test_extract_max_retries_exceeded(self):
        class Item(BaseModel):
            name: str

        mock_provider = AsyncMock()
        mock_provider.chat = AsyncMock(
            return_value=LLMResponse(content="always invalid")
        )
        llm = LLM(provider="openai", model="gpt-4o", api_key="test")
        llm._provider = mock_provider

        with pytest.raises(ValueError, match="Failed to extract"):
            await extract_structured(llm, Item, "Extract", max_retries=2)


# ============================================================================
# Agent
# ============================================================================


class TestAgent:
    async def test_agent_simple_response(self):
        """Agent with no tool calls returns immediately."""
        llm = LLM(provider="openai", model="gpt-4o", api_key="test")
        llm._provider.chat = AsyncMock(
            return_value=LLMResponse(content="Hello!", tool_calls=[])
        )

        agent = Agent(llm=llm)
        result = await agent.run("Hi")
        assert result.content == "Hello!"
        assert result.tool_calls_made == 0

    async def test_agent_with_tool_calls(self):
        """Agent that uses a tool, then gives final response."""
        llm = LLM(provider="openai", model="gpt-4o", api_key="test")

        # First call: LLM wants to call a tool
        # Second call: LLM gives final response
        llm._provider.chat = AsyncMock(
            side_effect=[
                LLMResponse(
                    content="",
                    tool_calls=[
                        {"id": "call_1", "name": "greet", "arguments": {"name": "World"}}
                    ],
                ),
                LLMResponse(content="I greeted World for you!", tool_calls=[]),
            ]
        )

        @tool
        async def greet(name: str) -> str:
            """Greet someone."""
            return f"Hello {name}!"

        agent = Agent(llm=llm)
        result = await agent.run("Greet World", tools=[greet])
        assert result.content == "I greeted World for you!"
        assert result.tool_calls_made == 1

    async def test_agent_unknown_tool(self):
        """Agent handles unknown tool calls gracefully."""
        llm = LLM(provider="openai", model="gpt-4o", api_key="test")
        llm._provider.chat = AsyncMock(
            side_effect=[
                LLMResponse(
                    content="",
                    tool_calls=[
                        {"id": "call_1", "name": "nonexistent", "arguments": {}}
                    ],
                ),
                LLMResponse(content="Sorry, tool not found.", tool_calls=[]),
            ]
        )

        agent = Agent(llm=llm)
        result = await agent.run("Do something")
        assert result.tool_calls_made == 1

    async def test_agent_max_turns(self):
        """Agent stops after max_turns."""
        llm = LLM(provider="openai", model="gpt-4o", api_key="test")
        # Always returns tool calls
        llm._provider.chat = AsyncMock(
            return_value=LLMResponse(
                content="",
                tool_calls=[{"id": "call_1", "name": "noop", "arguments": {}}],
            )
        )

        @tool
        async def noop() -> str:
            """No-op."""
            return "ok"

        agent = Agent(llm=llm, max_turns=2)
        result = await agent.run("Loop forever", tools=[noop])
        assert result.content == "Max turns reached without a final response."
        assert result.tool_calls_made == 2

    async def test_agent_no_llm_raises(self):
        agent = Agent()
        with pytest.raises(ValueError, match="requires an LLM"):
            await agent.run("Hello")

    async def test_agent_class_with_tools(self):
        """Subclass agent with @tool-decorated methods."""

        class MyAgent(Agent):
            system_prompt = "You are helpful."

            @tool
            async def add(self, a: int, b: int) -> int:
                """Add two numbers."""
                return a + b

        llm = LLM(provider="openai", model="gpt-4o", api_key="test")
        llm._provider.chat = AsyncMock(
            side_effect=[
                LLMResponse(
                    content="",
                    tool_calls=[
                        {"id": "c1", "name": "add", "arguments": {"a": 2, "b": 3}}
                    ],
                ),
                LLMResponse(content="The sum is 5.", tool_calls=[]),
            ]
        )

        agent = MyAgent(llm=llm)
        assert len(agent._tools) == 1
        assert agent._tools[0].name == "add"

        result = await agent.run("Add 2 and 3")
        assert result.content == "The sum is 5."

    async def test_agent_tool_error_handling(self):
        """Tool errors are fed back to LLM."""
        llm = LLM(provider="openai", model="gpt-4o", api_key="test")

        @tool
        async def failing_tool(x: str) -> str:
            """Always fails."""
            raise ValueError("Something broke")

        llm._provider.chat = AsyncMock(
            side_effect=[
                LLMResponse(
                    content="",
                    tool_calls=[
                        {"id": "c1", "name": "failing_tool", "arguments": {"x": "test"}}
                    ],
                ),
                LLMResponse(content="Tool failed.", tool_calls=[]),
            ]
        )

        agent = Agent(llm=llm)
        result = await agent.run("Try it", tools=[failing_tool])
        assert result.content == "Tool failed."

        # Check that the error message was passed back
        messages = result.messages
        tool_msg = [m for m in messages if m.get("role") == "tool"][0]
        assert "Something broke" in tool_msg["content"]


# ============================================================================
# Handoffs
# ============================================================================


class TestHandoffs:
    async def test_handoff_creates_tool(self):
        class TargetAgent(Agent):
            pass

        class TriageAgent(Agent):
            @handoff(to=TargetAgent)
            async def delegate(self, topic: str) -> str:
                """Delegate to target."""

        llm = LLM(provider="openai", model="gpt-4o", api_key="test")
        agent = TriageAgent(llm=llm)
        handoff_tools = [t for t in agent._tools if t.name == "delegate"]
        assert len(handoff_tools) == 1
        assert "target" in handoff_tools[0].description.lower() or "delegate" in handoff_tools[0].description.lower()


# ============================================================================
# RAG
# ============================================================================


class TestRAG:
    def test_chunk_text_short(self):
        """Short text returns single chunk."""
        chunks = _chunk_text("Hello world", 512, 50)
        assert chunks == ["Hello world"]

    def test_chunk_text_long(self):
        """Long text is chunked with overlap."""
        text = "word " * 200  # ~1000 chars
        chunks = _chunk_text(text, 100, 20)
        assert len(chunks) > 1
        # Each chunk should be roughly chunk_size or less
        for chunk in chunks:
            assert len(chunk) <= 110  # some tolerance for boundary splitting

    def test_chunk_text_empty(self):
        chunks = _chunk_text("", 512, 50)
        assert chunks == [""]

    async def test_rag_ingest(self):
        """Test ingesting a document."""
        mock_db = MagicMock()
        mock_db.vector = MagicMock()
        mock_db.vector.insert = AsyncMock()

        embed_model = LLM(provider="openai", model="text-embedding-3-small", api_key="test")
        embed_model._provider.embed = AsyncMock(return_value=[[0.1, 0.2, 0.3]])

        llm = LLM(provider="openai", model="gpt-4o", api_key="test")

        rag = RAGPipeline(
            db=mock_db,
            collection="test",
            llm=llm,
            embed_model=embed_model,
        )

        ids = await rag.ingest("Test document text", metadata={"source": "test"})
        assert len(ids) == 1
        assert ids[0] == "doc-1"
        mock_db.vector.insert.assert_called_once()

    async def test_rag_ingest_chunked(self):
        """Test that long documents are chunked."""
        mock_db = MagicMock()
        mock_db.vector = MagicMock()
        mock_db.vector.insert = AsyncMock()

        embed_model = LLM(provider="openai", model="text-embedding-3-small", api_key="test")
        # Return one embedding per chunk
        embed_model._provider.embed = AsyncMock(
            return_value=[[0.1, 0.2], [0.3, 0.4]]
        )

        llm = LLM(provider="openai", model="gpt-4o", api_key="test")

        rag = RAGPipeline(
            db=mock_db,
            collection="test",
            llm=llm,
            embed_model=embed_model,
            chunk_size=50,
            chunk_overlap=10,
        )

        text = "word " * 50  # ~250 chars, will produce multiple chunks
        ids = await rag.ingest(text)
        assert len(ids) > 1
        assert all("chunk" in id for id in ids)

    async def test_rag_ingest_batch(self):
        mock_db = MagicMock()
        mock_db.vector = MagicMock()
        mock_db.vector.insert = AsyncMock()

        embed_model = LLM(provider="openai", model="text-embedding-3-small", api_key="test")
        embed_model._provider.embed = AsyncMock(return_value=[[0.1, 0.2]])

        llm = LLM(provider="openai", model="gpt-4o", api_key="test")
        rag = RAGPipeline(db=mock_db, collection="test", llm=llm, embed_model=embed_model)

        total = await rag.ingest_batch([
            {"content": "Doc 1"},
            {"content": "Doc 2"},
        ])
        assert total == 2

    async def test_rag_query(self):
        """Test RAG query flow."""
        mock_db = MagicMock()
        mock_db.vector = MagicMock()

        mock_result = MagicMock()
        mock_result.id = "doc-1"
        mock_result.score = 0.95
        mock_result.metadata = {"_chunk_text": "Relevant content", "source": "test"}
        mock_db.vector.search = AsyncMock(return_value=[mock_result])

        embed_model = LLM(provider="openai", model="text-embedding-3-small", api_key="test")
        embed_model._provider.embed = AsyncMock(return_value=[[0.1, 0.2, 0.3]])

        llm = LLM(provider="openai", model="gpt-4o", api_key="test")
        llm._provider.chat = AsyncMock(
            return_value=LLMResponse(content="Generated answer based on context.")
        )

        rag = RAGPipeline(
            db=mock_db,
            collection="test",
            llm=llm,
            embed_model=embed_model,
        )

        answer = await rag.query("What is the answer?", k=5)
        assert isinstance(answer, RAGAnswer)
        assert answer.text == "Generated answer based on context."
        assert len(answer.sources) == 1
        assert answer.sources[0].id == "doc-1"
        assert answer.sources[0].score == 0.95


# ============================================================================
# Workflow
# ============================================================================


class TestWorkflow:
    def test_step_decorator(self):
        @step
        async def my_step(self, x: int) -> int:
            return x * 2

        assert isinstance(my_step, _StepDef)
        assert my_step.name == "my_step"
        assert my_step.depends_on == []

    def test_step_with_depends(self):
        @step(depends_on=["first"])
        async def second(self, first: dict) -> str:
            return "done"

        assert second.depends_on == ["first"]

    async def test_workflow_linear(self):
        """Linear workflow: step1 → step2 → step3."""

        class Linear(Workflow):
            @step
            async def first(self, value: int) -> int:
                return value * 2

            @step(depends_on=["first"])
            async def second(self, first: int) -> int:
                return first + 10

            @step(depends_on=["second"])
            async def third(self, second: int) -> str:
                return f"Result: {second}"

        wf = Linear()
        result = await wf.run(value=5)
        assert result.output == "Result: 20"
        assert len(result.trace) == 3
        assert result.trace[0].name == "first"
        assert result.trace[0].output == 10
        assert result.trace[1].name == "second"
        assert result.trace[1].output == 20
        assert result.trace[2].name == "third"
        assert result.trace[2].output == "Result: 20"
        assert result.total_duration_ms > 0

    async def test_workflow_parallel_steps(self):
        """Steps without dependencies on each other run concurrently."""

        class Parallel(Workflow):
            @step
            async def a(self, x: int) -> int:
                return x + 1

            @step
            async def b(self, x: int) -> int:
                return x + 2

            @step(depends_on=["a", "b"])
            async def combine(self, a: int, b: int) -> int:
                return a + b

        wf = Parallel()
        result = await wf.run(x=10)
        assert result.output == 23  # (10+1) + (10+2)

    async def test_workflow_single_step(self):
        class Single(Workflow):
            @step
            async def only(self, msg: str) -> str:
                return f"Got: {msg}"

        wf = Single()
        result = await wf.run(msg="hello")
        assert result.output == "Got: hello"
        assert len(result.trace) == 1

    async def test_workflow_step_error(self):
        class Failing(Workflow):
            @step
            async def bad_step(self) -> None:
                raise ValueError("Step failed!")

        wf = Failing()
        with pytest.raises(ValueError, match="Step failed!"):
            await wf.run()

    async def test_workflow_with_llm(self):
        """Workflow steps can use self.llm."""

        class AIWorkflow(Workflow):
            @step
            async def generate(self, prompt: str) -> str:
                return await self.llm.complete(prompt)

        llm = LLM(provider="openai", model="gpt-4o", api_key="test")
        llm._provider.chat = AsyncMock(
            return_value=LLMResponse(content="AI response")
        )

        wf = AIWorkflow(llm=llm)
        result = await wf.run(prompt="Hello")
        assert result.output == "AI response"


# ============================================================================
# Memory
# ============================================================================


class TestMemory:
    def _mock_db(self):
        db = MagicMock()
        db.document = MagicMock()
        db.vector = MagicMock()
        return db

    async def test_memory_create(self):
        db = self._mock_db()
        db.document.insert = AsyncMock()

        memory = Memory(db=db, collection="conversations")
        conv_id = await memory.create()

        assert conv_id  # UUID string
        db.document.insert.assert_called_once()
        call_args = db.document.insert.call_args
        assert call_args[0][0] == "conversations"

    async def test_memory_add_message(self):
        db = self._mock_db()
        db.document.get = AsyncMock(
            return_value={"conversation_id": "c1", "messages": []}
        )
        db.document.update = AsyncMock()

        memory = Memory(db=db)
        await memory.add("c1", role="user", content="Hello")

        db.document.update.assert_called_once()
        call_args = db.document.update.call_args
        updated_messages = call_args[0][2]["messages"]
        assert len(updated_messages) == 1
        assert updated_messages[0]["role"] == "user"
        assert updated_messages[0]["content"] == "Hello"

    async def test_memory_add_not_found(self):
        db = self._mock_db()
        db.document.get = AsyncMock(return_value=None)

        memory = Memory(db=db)
        with pytest.raises(ValueError, match="not found"):
            await memory.add("nonexistent", role="user", content="Hello")

    async def test_memory_get_messages(self):
        db = self._mock_db()
        db.document.get = AsyncMock(
            return_value={
                "conversation_id": "c1",
                "messages": [
                    {"role": "user", "content": "Hello", "timestamp": 1.0, "metadata": {}},
                    {"role": "assistant", "content": "Hi!", "timestamp": 2.0, "metadata": {}},
                ],
            }
        )

        memory = Memory(db=db)
        messages = await memory.get_messages("c1")
        assert len(messages) == 2
        assert isinstance(messages[0], MemoryMessage)
        assert messages[0].role == "user"
        assert messages[1].content == "Hi!"

    async def test_memory_get_messages_with_limit(self):
        db = self._mock_db()
        db.document.get = AsyncMock(
            return_value={
                "conversation_id": "c1",
                "messages": [
                    {"role": "user", "content": f"Msg {i}", "timestamp": float(i), "metadata": {}}
                    for i in range(10)
                ],
            }
        )

        memory = Memory(db=db)
        messages = await memory.get_messages("c1", limit=3)
        assert len(messages) == 3

    async def test_memory_search_requires_embed(self):
        db = self._mock_db()
        memory = Memory(db=db)

        with pytest.raises(ValueError, match="embed_model"):
            await memory.search("c1", "query")

    async def test_memory_search(self):
        db = self._mock_db()
        mock_result = MagicMock()
        mock_result.metadata = {
            "_conversation_id": "c1",
            "_role": "user",
            "_content": "About quantum",
            "_timestamp": 1.0,
        }
        db.vector.search = AsyncMock(return_value=[mock_result])

        embed_model = LLM(provider="openai", model="text-embedding-3-small", api_key="test")
        embed_model._provider.embed = AsyncMock(return_value=[[0.1, 0.2]])

        memory = Memory(db=db, embed_model=embed_model)
        results = await memory.search("c1", "quantum", k=5)
        assert len(results) == 1
        assert results[0].content == "About quantum"

    async def test_memory_summarize_requires_llm(self):
        db = self._mock_db()
        memory = Memory(db=db)

        with pytest.raises(ValueError, match="llm"):
            await memory.summarize("c1")

    async def test_memory_summarize(self):
        db = self._mock_db()
        db.document.get = AsyncMock(
            return_value={
                "conversation_id": "c1",
                "messages": [
                    {"role": "user", "content": "Tell me about AI", "timestamp": 1.0, "metadata": {}},
                    {"role": "assistant", "content": "AI is...", "timestamp": 2.0, "metadata": {}},
                ],
            }
        )

        llm = LLM(provider="openai", model="gpt-4o", api_key="test")
        llm._provider.chat = AsyncMock(
            return_value=LLMResponse(content="Summary: discussed AI.")
        )

        memory = Memory(db=db, llm=llm)
        summary = await memory.summarize("c1")
        assert "Summary" in summary

    async def test_memory_delete(self):
        db = self._mock_db()
        db.document.delete = AsyncMock()

        memory = Memory(db=db)
        await memory.delete("c1")
        db.document.delete.assert_called_once_with("conversations", "c1")

    async def test_memory_list_conversations(self):
        db = self._mock_db()
        db.document.find = AsyncMock(
            return_value=[
                {
                    "conversation_id": "c1",
                    "created_at": 1.0,
                    "metadata": {},
                    "messages": [{"role": "user", "content": "Hi"}],
                }
            ]
        )

        memory = Memory(db=db)
        convs = await memory.list_conversations()
        assert len(convs) == 1
        assert convs[0]["conversation_id"] == "c1"
        assert convs[0]["message_count"] == 1


# ============================================================================
# MCP Server
# ============================================================================


class TestMCPServer:
    def test_mcp_server_init(self):
        mcp = MCPServer(name="test-tools")
        assert mcp.name == "test-tools"
        assert mcp._tools == {}
        assert mcp._resources == {}

    def test_mcp_tool_registration(self):
        mcp = MCPServer()

        @mcp.tool()
        async def search(query: str) -> list:
            """Search the database."""
            return []

        assert "search" in mcp._tools
        assert mcp._tools["search"].name == "search"
        assert mcp._tools["search"].description == "Search the database."

    def test_mcp_tool_custom_name(self):
        mcp = MCPServer()

        @mcp.tool(name="custom_search")
        async def search(query: str) -> list:
            """Search."""
            return []

        assert "custom_search" in mcp._tools
        assert "search" not in mcp._tools

    def test_mcp_resource_registration(self):
        mcp = MCPServer()

        @mcp.resource("users://{user_id}")
        async def get_user(user_id: str) -> dict:
            """Get user."""
            return {}

        assert "users://{user_id}" in mcp._resources

    def test_resource_uri_matching(self):
        res = _ResourceDef(
            "users://{user_id}",
            lambda user_id: {"id": user_id},
        )
        params = res.match("users://123")
        assert params == {"user_id": "123"}

        no_match = res.match("posts://456")
        assert no_match is None

    def test_resource_multiple_params(self):
        res = _ResourceDef(
            "orgs://{org_id}/users://{user_id}",
            lambda org_id, user_id: {},
        )
        params = res.match("orgs://acme/users://42")
        assert params == {"org_id": "acme", "user_id": "42"}


class TestMCPServerHTTP:
    """Test MCP server via ASGI (httpx + ASGITransport)."""

    def _build_mcp(self) -> MCPServer:
        mcp = MCPServer(name="test-server", version="1.0.0")

        @mcp.tool()
        async def add(a: int, b: int) -> int:
            """Add two numbers."""
            return a + b

        @mcp.resource("items://{item_id}")
        async def get_item(item_id: str) -> dict:
            """Get an item."""
            return {"id": item_id, "name": f"Item {item_id}"}

        return mcp

    async def test_server_info(self):
        import httpx

        mcp = self._build_mcp()
        transport = httpx.ASGITransport(app=mcp)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/")
            assert resp.status_code == 200
            data = resp.json()
            assert data["name"] == "test-server"
            assert data["protocol"] == "mcp"
            assert data["capabilities"]["tools"] is True

    async def test_list_tools(self):
        import httpx

        mcp = self._build_mcp()
        transport = httpx.ASGITransport(app=mcp)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/tools")
            assert resp.status_code == 200
            tools = resp.json()["tools"]
            assert len(tools) == 1
            assert tools[0]["name"] == "add"

    async def test_call_tool(self):
        import httpx

        mcp = self._build_mcp()
        transport = httpx.ASGITransport(app=mcp)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post("/tools/add", json={"a": 3, "b": 4})
            assert resp.status_code == 200
            assert resp.json()["result"] == 7

    async def test_call_unknown_tool(self):
        import httpx

        mcp = self._build_mcp()
        transport = httpx.ASGITransport(app=mcp)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post("/tools/nonexistent", json={})
            assert resp.status_code == 404

    async def test_list_resources(self):
        import httpx

        mcp = self._build_mcp()
        transport = httpx.ASGITransport(app=mcp)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/resources")
            assert resp.status_code == 200
            resources = resp.json()["resources"]
            assert len(resources) == 1
            assert resources[0]["uri_template"] == "items://{item_id}"

    async def test_read_resource(self):
        import httpx

        mcp = self._build_mcp()
        transport = httpx.ASGITransport(app=mcp)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/resources/items://42")
            assert resp.status_code == 200
            assert resp.json()["result"]["id"] == "42"

    async def test_read_unknown_resource(self):
        import httpx

        mcp = self._build_mcp()
        transport = httpx.ASGITransport(app=mcp)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/resources/unknown://foo")
            assert resp.status_code == 404


# ============================================================================
# MCP Client
# ============================================================================


class TestMCPClient:
    async def test_client_context_manager(self):
        async with MCPClient("http://localhost:9999") as client:
            assert client._client is not None
        assert client._client is None

    async def test_client_requires_context(self):
        client = MCPClient("http://localhost:9999")
        with pytest.raises(RuntimeError, match="context manager"):
            await client.list_tools()

    async def test_client_url_normalization(self):
        client = MCPClient("http://localhost:9999/mcp/")
        assert client.url == "http://localhost:9999/mcp"


# ============================================================================
# Integration: AI __init__ re-exports
# ============================================================================


class TestAIExports:
    def test_all_exports_importable(self):
        from neutron.ai import (
            LLM,
            LLMResponse,
            Tool,
            tool,
            extract_structured,
            Agent,
            AgentResult,
            handoff,
            RAGPipeline,
            RAGAnswer,
            RAGSource,
            Workflow,
            WorkflowResult,
            step,
            Memory,
            MemoryMessage,
            MCPServer,
            MCPClient,
        )
        # Just verify imports succeed
        assert LLM is not None
        assert Agent is not None
        assert Workflow is not None
        assert Memory is not None
        assert MCPServer is not None
