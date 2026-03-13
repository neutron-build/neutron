"""RAG pipeline — embed, store, retrieve, generate."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from neutron.ai.providers import LLM


class RAGSource(BaseModel):
    id: str
    score: float
    content: str = ""
    metadata: dict[str, Any] = {}


class RAGAnswer(BaseModel):
    text: str
    sources: list[RAGSource] = []
    metadata: dict[str, Any] = {}


class RAGPipeline:
    """Retrieval-Augmented Generation pipeline.

    Orchestrates: embed query → retrieve from vector store → generate answer.

    Usage::

        rag = RAGPipeline(
            db=db,
            collection="knowledge",
            llm=LLM(provider="openai", model="gpt-4o"),
            embed_model=LLM(provider="openai", model="text-embedding-3-small"),
        )
        await rag.ingest("Document text...", metadata={"source": "file.pdf"})
        answer = await rag.query("What is ...?", k=10)
    """

    def __init__(
        self,
        db: Any,
        collection: str,
        llm: LLM,
        embed_model: LLM,
        *,
        chunk_size: int = 512,
        chunk_overlap: int = 50,
    ) -> None:
        self.db = db
        self.collection = collection
        self.llm = llm
        self.embed_model = embed_model
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self._doc_counter = 0

    async def ingest(
        self,
        text: str,
        *,
        metadata: dict[str, Any] | None = None,
        doc_id: str | None = None,
    ) -> list[str]:
        """Ingest text by chunking, embedding, and storing in the vector collection."""
        chunks = _chunk_text(text, self.chunk_size, self.chunk_overlap)
        embeddings = await self.embed_model.embed(chunks)

        ids: list[str] = []
        for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
            self._doc_counter += 1
            chunk_id = doc_id or f"doc-{self._doc_counter}"
            if len(chunks) > 1:
                chunk_id = f"{chunk_id}-chunk-{i}"
            meta = dict(metadata or {})
            meta["_chunk_text"] = chunk
            meta["_chunk_index"] = i
            await self.db.vector.insert(
                self.collection, chunk_id, embedding, meta
            )
            ids.append(chunk_id)

        return ids

    async def ingest_file(
        self,
        file_path: str,
        *,
        metadata: dict[str, Any] | None = None,
        doc_id: str | None = None,
        encoding: str = "utf-8",
    ) -> list[str]:
        """Ingest a text file by reading it and passing to ``ingest()``.

        Args:
            file_path: Path to the text file to ingest.
            metadata: Optional metadata dict; ``source`` defaults to ``file_path``.
            doc_id: Optional stable document ID (chunks are suffixed ``-chunk-N``).
            encoding: File encoding (default ``utf-8``).

        Returns:
            List of chunk IDs stored in the vector collection.
        """
        with open(file_path, "r", encoding=encoding) as fh:
            text = fh.read()
        meta = dict(metadata or {})
        meta.setdefault("source", file_path)
        return await self.ingest(text, metadata=meta, doc_id=doc_id)

    async def ingest_batch(
        self,
        documents: list[dict[str, Any]],
    ) -> int:
        """Batch ingest documents. Each dict should have 'content' and optional 'metadata'."""
        total = 0
        for doc in documents:
            ids = await self.ingest(
                doc["content"], metadata=doc.get("metadata")
            )
            total += len(ids)
        return total

    async def query(
        self,
        question: str,
        *,
        k: int = 10,
        rerank: bool = False,
    ) -> RAGAnswer:
        """Query the knowledge base with RAG.

        1. Embed the question
        2. Retrieve top-k chunks from the vector store
        3. Generate an answer using the LLM with retrieved context
        """
        # 1. Embed
        query_embedding = (await self.embed_model.embed(question))[0]

        # 2. Retrieve
        results = await self.db.vector.search(
            self.collection, query_embedding, k=k
        )

        sources: list[RAGSource] = []
        context_parts: list[str] = []
        for r in results:
            chunk_text = r.metadata.get("_chunk_text", "")
            sources.append(
                RAGSource(
                    id=r.id,
                    score=r.score,
                    content=chunk_text,
                    metadata={
                        k: v
                        for k, v in r.metadata.items()
                        if not k.startswith("_")
                    },
                )
            )
            if chunk_text:
                context_parts.append(chunk_text)

        # 3. Rerank (simple re-scoring via LLM if requested)
        if rerank and len(sources) > 1:
            sources.sort(key=lambda s: s.score)

        # 4. Generate
        context = "\n\n---\n\n".join(context_parts)
        system_prompt = (
            "You are a helpful assistant. Answer the user's question based on "
            "the following context. If the context doesn't contain relevant "
            "information, say so. Cite sources when possible.\n\n"
            f"Context:\n{context}"
        )

        llm_resp = await self.llm.chat(
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": question},
            ],
            temperature=0.3,
        )

        return RAGAnswer(
            text=llm_resp.content,
            sources=sources,
            metadata={"k": k, "chunks_retrieved": len(sources)},
        )


# ---------------------------------------------------------------------------
# Text chunking
# ---------------------------------------------------------------------------


def _chunk_text(text: str, chunk_size: int, overlap: int) -> list[str]:
    """Split text into overlapping chunks by character count."""
    if len(text) <= chunk_size:
        return [text]
    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        # Try to break on a sentence or word boundary
        if end < len(text):
            last_period = chunk.rfind(". ")
            last_newline = chunk.rfind("\n")
            break_at = max(last_period, last_newline)
            if break_at > chunk_size // 2:
                chunk = chunk[: break_at + 1]
                end = start + break_at + 1
        chunks.append(chunk.strip())
        start = end - overlap
    return [c for c in chunks if c]
