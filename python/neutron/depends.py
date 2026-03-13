"""Dependency injection system."""

from __future__ import annotations

from typing import Any, Callable


class _Depends:
    """Dependency injection marker."""

    def __init__(self, dependency: Callable[..., Any]) -> None:
        self.dependency = dependency

    def __repr__(self) -> str:
        return f"Depends({self.dependency.__name__})"


def Depends(dependency: Callable[..., Any]) -> Any:  # noqa: N802
    """Declare a dependency that will be resolved at request time.

    Usage::

        async def get_db(request: Request) -> NucleusClient:
            return request.app.state.db

        @router.get("/users")
        async def list_users(db: NucleusClient = Depends(get_db)) -> list[User]:
            ...
    """
    return _Depends(dependency)
