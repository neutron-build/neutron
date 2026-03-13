"""Structured output — Instructor pattern with Pydantic models."""

from __future__ import annotations

import json
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

T = TypeVar("T", bound=BaseModel)


async def extract_structured(
    llm: Any,  # LLM instance (avoid circular import)
    model: type[T],
    prompt: str,
    *,
    max_retries: int = 3,
    **kwargs: Any,
) -> T:
    """Extract structured data from text into a Pydantic model.

    Uses provider-native JSON mode when available, with Instructor-style
    retry logic: up to ``max_retries`` attempts, feeding Pydantic validation
    errors back to the LLM.
    """
    schema = model.model_json_schema()
    schema_str = json.dumps(schema, indent=2)

    system_msg = (
        "You are a structured data extraction assistant. "
        "You MUST respond with ONLY valid JSON that conforms to the following schema. "
        "Do NOT include any text outside the JSON object.\n\n"
        f"JSON Schema:\n```json\n{schema_str}\n```"
    )

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": prompt},
    ]

    last_error: Exception | None = None

    for attempt in range(max_retries):
        response = await llm.chat(messages, **kwargs)
        content = response.content.strip()

        # Strip markdown code fences if present
        if content.startswith("```"):
            lines = content.split("\n")
            # Remove first and last lines (the fences)
            lines = [l for l in lines if not l.strip().startswith("```")]
            content = "\n".join(lines).strip()

        try:
            data = json.loads(content)
            return model.model_validate(data)
        except (json.JSONDecodeError, ValidationError) as e:
            last_error = e
            error_msg = str(e)
            messages.append({"role": "assistant", "content": response.content})
            messages.append(
                {
                    "role": "user",
                    "content": (
                        f"Your response failed validation:\n{error_msg}\n\n"
                        "Please fix the errors and respond with ONLY valid JSON."
                    ),
                }
            )

    raise ValueError(
        f"Failed to extract {model.__name__} after {max_retries} attempts. "
        f"Last error: {last_error}"
    )
