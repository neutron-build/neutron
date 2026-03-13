"""Conversation memory — Nucleus-backed message storage with semantic search."""

from __future__ import annotations

import time
import uuid
from typing import Any

from pydantic import BaseModel

from neutron.ai.providers import LLM


class MemoryMessage(BaseModel):
    role: str
    content: str
    timestamp: float = 0.0
    metadata: dict[str, Any] = {}


class Memory:
    """Nucleus-backed conversation memory.

    Stores conversations in a document collection, with optional
    vector-based semantic search over message history.

    Usage::

        memory = Memory(db=db, collection="conversations")

        conv_id = await memory.create()
        await memory.add(conv_id, role="user", content="Hello!")
        await memory.add(conv_id, role="assistant", content="Hi!")

        messages = await memory.get_messages(conv_id, limit=50)
        relevant = await memory.search(conv_id, "quantum computing", k=5)
    """

    def __init__(
        self,
        db: Any,
        collection: str = "conversations",
        llm: LLM | None = None,
        embed_model: LLM | None = None,
    ) -> None:
        self.db = db
        self.collection = collection
        self.llm = llm
        self.embed_model = embed_model

    async def create(self, metadata: dict[str, Any] | None = None) -> str:
        """Create a new conversation and return its ID."""
        conversation_id = str(uuid.uuid4())
        doc = {
            "conversation_id": conversation_id,
            "messages": [],
            "created_at": time.time(),
            "metadata": metadata or {},
        }
        await self.db.document.insert(self.collection, doc, doc_id=conversation_id)
        return conversation_id

    async def add(
        self,
        conversation_id: str,
        role: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Add a message to a conversation."""
        msg = {
            "role": role,
            "content": content,
            "timestamp": time.time(),
            "metadata": metadata or {},
        }

        # Get existing conversation doc
        doc = await self.db.document.get(self.collection, conversation_id)
        if doc is None:
            raise ValueError(f"Conversation {conversation_id} not found")

        messages = doc.get("messages", [])
        messages.append(msg)

        await self.db.document.update(
            self.collection, conversation_id, {"messages": messages}
        )

        # If we have an embed model, also store in vector collection for search
        if self.embed_model is not None:
            embeddings = await self.embed_model.embed(content)
            vector_id = f"{conversation_id}-{len(messages) - 1}"
            await self.db.vector.insert(
                f"{self.collection}_vectors",
                vector_id,
                embeddings[0],
                {
                    "_conversation_id": conversation_id,
                    "_role": role,
                    "_content": content,
                    "_timestamp": msg["timestamp"],
                },
            )

    async def get_messages(
        self,
        conversation_id: str,
        *,
        limit: int = 50,
        offset: int = 0,
    ) -> list[MemoryMessage]:
        """Get messages from a conversation, most recent first."""
        doc = await self.db.document.get(self.collection, conversation_id)
        if doc is None:
            raise ValueError(f"Conversation {conversation_id} not found")

        messages = doc.get("messages", [])
        # Return in chronological order with optional pagination
        sliced = messages[offset : offset + limit] if limit else messages[offset:]
        return [MemoryMessage(**m) for m in sliced]

    async def search(
        self,
        conversation_id: str,
        query: str,
        *,
        k: int = 5,
    ) -> list[MemoryMessage]:
        """Semantic search over conversation history.

        Requires an ``embed_model`` to be configured.
        """
        if self.embed_model is None:
            raise ValueError("Memory.search requires an embed_model")

        query_embedding = (await self.embed_model.embed(query))[0]
        results = await self.db.vector.search(
            f"{self.collection}_vectors", query_embedding, k=k * 2
        )

        # Filter to this conversation
        messages: list[MemoryMessage] = []
        for r in results:
            if r.metadata.get("_conversation_id") != conversation_id:
                continue
            messages.append(
                MemoryMessage(
                    role=r.metadata.get("_role", ""),
                    content=r.metadata.get("_content", ""),
                    timestamp=r.metadata.get("_timestamp", 0.0),
                )
            )
            if len(messages) >= k:
                break

        return messages

    async def summarize(
        self,
        conversation_id: str,
        *,
        max_messages: int | None = None,
    ) -> str:
        """Summarize a conversation using the LLM.

        Requires an ``llm`` to be configured.
        """
        if self.llm is None:
            raise ValueError("Memory.summarize requires an llm")

        messages = await self.get_messages(conversation_id, limit=max_messages or 200)
        if not messages:
            return ""

        transcript = "\n".join(
            f"{m.role}: {m.content}" for m in messages
        )

        summary = await self.llm.complete(
            f"Summarize the following conversation concisely:\n\n{transcript}"
        )
        return summary

    async def delete(self, conversation_id: str) -> None:
        """Delete a conversation and its vector embeddings."""
        await self.db.document.delete(self.collection, conversation_id)

    async def list_conversations(
        self, *, limit: int = 50
    ) -> list[dict[str, Any]]:
        """List recent conversations (metadata only, no messages)."""
        docs = await self.db.document.find(self.collection, {}, limit=limit)
        return [
            {
                "conversation_id": d.get("conversation_id"),
                "created_at": d.get("created_at"),
                "metadata": d.get("metadata", {}),
                "message_count": len(d.get("messages", [])),
            }
            for d in docs
        ]
