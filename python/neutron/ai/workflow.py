"""Workflow engine — DAG-based multi-step execution with @step decorator."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable

from pydantic import BaseModel

from neutron.ai.providers import LLM


class StepResult(BaseModel):
    name: str
    output: Any = None
    duration_ms: float = 0.0
    error: str | None = None


class WorkflowResult(BaseModel):
    output: Any = None
    trace: list[StepResult] = []
    total_duration_ms: float = 0.0


class _StepDef:
    """Marker for methods decorated with @step."""

    def __init__(self, fn: Callable, depends_on: list[str] | None = None) -> None:
        self.fn = fn
        self.name = fn.__name__
        self.description = (fn.__doc__ or "").strip()
        self.depends_on: list[str] = depends_on or []


def step(
    fn: Callable | None = None, *, depends_on: list[str] | None = None
) -> Any:
    """Decorator to mark a method as a workflow step.

    Usage::

        class MyWorkflow(Workflow):
            @step
            async def first(self, topic: str) -> dict: ...

            @step(depends_on=["first"])
            async def second(self, first: dict) -> str: ...
    """
    if fn is not None:
        return _StepDef(fn, depends_on=depends_on)

    def decorator(f: Callable) -> _StepDef:
        return _StepDef(f, depends_on=depends_on)

    return decorator


class Workflow:
    """DAG-based multi-step workflow execution.

    Subclass and define steps with the ``@step`` decorator::

        class ContentPipeline(Workflow):
            @step
            async def research(self, topic: str) -> dict:
                ...

            @step(depends_on=["research"])
            async def write(self, research: dict) -> str:
                ...

        pipeline = ContentPipeline(llm=llm)
        result = await pipeline.run(topic="Quantum Computing")
    """

    llm: LLM | None = None

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

        # Collect steps from class
        self._steps: dict[str, _StepDef] = {}
        for attr_name in dir(self.__class__):
            attr = getattr(self.__class__, attr_name, None)
            if isinstance(attr, _StepDef):
                self._steps[attr.name] = attr

    async def run(self, **kwargs: Any) -> WorkflowResult:
        """Execute the workflow DAG.

        Steps without dependencies run first; their outputs are passed as
        keyword arguments to dependent steps.
        """
        start = time.monotonic()
        outputs: dict[str, Any] = {}
        trace: list[StepResult] = []
        executed: set[str] = set()

        # Topological execution
        remaining = set(self._steps.keys())
        while remaining:
            # Find steps whose dependencies are all satisfied
            ready = [
                name
                for name in remaining
                if all(dep in executed for dep in self._steps[name].depends_on)
            ]
            if not ready:
                unmet = {
                    name: [d for d in self._steps[name].depends_on if d not in executed]
                    for name in remaining
                }
                raise ValueError(f"Workflow has unresolvable dependencies: {unmet}")

            # Execute ready steps concurrently
            tasks = []
            for name in ready:
                tasks.append(self._execute_step(name, outputs, kwargs))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for name, result in zip(ready, results):
                if isinstance(result, Exception):
                    step_result = StepResult(
                        name=name, error=str(result), duration_ms=0.0
                    )
                    trace.append(step_result)
                    raise result
                else:
                    output, duration = result
                    outputs[name] = output
                    trace.append(
                        StepResult(name=name, output=output, duration_ms=duration)
                    )
                    executed.add(name)
                    remaining.discard(name)

        total = (time.monotonic() - start) * 1000

        # The final output is the last step's output (by execution order)
        final_output = trace[-1].output if trace else None

        return WorkflowResult(
            output=final_output, trace=trace, total_duration_ms=total
        )

    async def _execute_step(
        self,
        name: str,
        outputs: dict[str, Any],
        initial_kwargs: dict[str, Any],
    ) -> tuple[Any, float]:
        """Execute a single step, injecting dependency outputs."""
        step_def = self._steps[name]
        start = time.monotonic()

        # Build kwargs: initial run kwargs + dependency outputs
        call_kwargs: dict[str, Any] = {}

        # If this step has dependencies, pass their outputs
        for dep in step_def.depends_on:
            if dep in outputs:
                call_kwargs[dep] = outputs[dep]

        # Also pass initial kwargs for root steps
        if not step_def.depends_on:
            call_kwargs.update(initial_kwargs)

        result = await step_def.fn(self, **call_kwargs)
        duration = (time.monotonic() - start) * 1000
        return result, duration
