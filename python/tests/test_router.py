"""Tests for Router registration and route collection."""

from neutron.router import Router


def test_register_get_route():
    router = Router()

    @router.get("/users")
    async def list_users() -> list:
        return []

    routes = router.get_routes()
    assert len(routes) == 1
    assert routes[0].path == "/users"
    assert "GET" in routes[0].methods


def test_register_post_route():
    router = Router()

    @router.post("/users")
    async def create_user() -> dict:
        return {}

    routes = router.get_routes()
    assert len(routes) == 1
    assert "POST" in routes[0].methods


def test_prefix_applied():
    router = Router()

    @router.get("/items")
    async def list_items() -> list:
        return []

    routes = router.get_routes(prefix="/api")
    assert routes[0].path == "/api/items"


def test_handler_info_collected():
    router = Router()

    @router.get("/users/{user_id}", summary="Get a user", tags=["users"])
    async def get_user(user_id: int) -> dict:
        return {}

    info = router.get_handler_info()
    assert len(info) == 1
    assert info[0]["path"] == "/users/{user_id}"
    assert info[0]["method"] == "get"
    assert info[0]["summary"] == "Get a user"
    assert info[0]["tags"] == ["users"]


def test_handler_info_prefix():
    router = Router()

    @router.get("/items")
    async def items() -> list:
        return []

    info = router.get_handler_info(prefix="/v1")
    assert info[0]["path"] == "/v1/items"


def test_group_sub_router():
    router = Router()
    sub = router.group("/admin")

    @sub.get("/stats")
    async def stats() -> dict:
        return {}

    routes = router.get_routes()
    assert len(routes) == 1
    assert routes[0].path == "/admin/stats"


def test_all_http_methods():
    router = Router()

    @router.get("/a")
    async def a() -> dict:
        return {}

    @router.post("/b")
    async def b() -> dict:
        return {}

    @router.put("/c")
    async def c() -> dict:
        return {}

    @router.patch("/d")
    async def d() -> dict:
        return {}

    @router.delete("/e")
    async def e() -> dict:
        return {}

    routes = router.get_routes()
    all_methods = set()
    for r in routes:
        all_methods.update(r.methods)
    assert "GET" in all_methods
    assert "POST" in all_methods
    assert "PUT" in all_methods
    assert "PATCH" in all_methods
    assert "DELETE" in all_methods


def test_multiple_routers_combined():
    r1 = Router()
    r2 = Router()

    @r1.get("/users")
    async def users() -> list:
        return []

    @r2.get("/posts")
    async def posts() -> list:
        return []

    all_routes = r1.get_routes("/api") + r2.get_routes("/api")
    paths = {r.path for r in all_routes}
    assert "/api/users" in paths
    assert "/api/posts" in paths
