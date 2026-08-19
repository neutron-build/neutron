"""Shared scenario semantics for the ASGI benchmark apps.

Every number here is a port of the TypeScript harness's route implementations
(``typescript/benchmarks/next-app/pages/*`` and the scenario definitions in
``typescript/benchmarks/run-comparison.mjs``). The four apps below must do the
SAME work per scenario, byte-identically where the response is a constant, so
that differences between them are framework overhead, not scenario drift.

Reference semantics (verified against the TS source, not from memory):

- compute:  ``acc = (acc + (i*17) % 97) % 1000003`` for i in 0..139999
            (identical in next-app, astro-app, remix, remix3-fw; JS and Python
            integer arithmetic agree exactly over this range)
- mutate:   32-bit LCG ``acc = (acc*1664525 + 1013904223) >>> 0``, seed 13,
            repeat 6000 clamped to [1, 50000]
- big:      400 rows of ``row-{i+1}-{(i*37) % 101}`` rendered as <li> items
- auth:     literal header comparison against ``Bearer valid-token``
- bodies:   the harness sends exactly ``{"seed":13,"repeat":6000}`` to
            /api/mutate and ``{}`` to /api/session/refresh
"""

VALID_TOKEN = "valid-token"
AUTH_HEADER = f"Bearer {VALID_TOKEN}"

USERS = {
    "1": ("Alice", "alice@example.com"),
    "2": ("Bob", "bob@example.com"),
    "3": ("Charlie", "charlie@example.com"),
}

COMPUTE_ITERATIONS = 140_000
MUTATE_DEFAULT_SEED = 13
MUTATE_DEFAULT_REPEAT = 6000
MUTATE_MAX_REPEAT = 50_000
BIG_ROWS = 400

STATIC_HTML = (
    "<main><h1>bench-home</h1>"
    "<p>Framework benchmark static route.</p></main>"
)
LOGIN_HTML = (
    "<main><h1>bench-login</h1>"
    "<p>Send Authorization: Bearer valid-token to access /protected.</p></main>"
)


def bench_compute(iterations: int = COMPUTE_ITERATIONS) -> int:
    """Port of next-app/pages/compute.js benchCompute()."""
    acc = 0
    for i in range(iterations):
        acc = (acc + (i * 17) % 97) % 1000003
    return acc


def run_mutation(seed: int = MUTATE_DEFAULT_SEED, repeat: int = MUTATE_DEFAULT_REPEAT) -> int:
    """Port of next-app/pages/api/mutate.js runMutation() (32-bit unsigned wrap)."""
    acc = seed & 0xFFFFFFFF
    for _ in range(repeat):
        acc = (acc * 1664525 + 1013904223) & 0xFFFFFFFF
    return acc


def clamp_mutation(seed: int, repeat: int) -> tuple[int, int]:
    """Port of mutate.js input normalisation (non-finite handling omitted:
    the harness always sends finite ints; pydantic/litestar reject the rest)."""
    safe_seed = MUTATE_DEFAULT_SEED if seed is None else seed
    safe_repeat = max(1, min(MUTATE_MAX_REPEAT, repeat))
    return safe_seed, safe_repeat


def user_html(user_id: str) -> str:
    """Port of next-app/pages/users/[id].js UserPage."""
    name, email = USERS.get(user_id, ("Unknown", "unknown@example.com"))
    return (
        f"<main><h1>User: {name}</h1>"
        f"<p>ID: {user_id}</p>"
        f"<p>Email: {email}</p></main>"
    )


def compute_html(value: int) -> str:
    """Port of next-app/pages/compute.js ComputePage."""
    return f"<main><h1>Compute</h1><p>value={value}</p></main>"


def big_html() -> str:
    """Port of next-app/pages/big.js BigPage (400 rows, built per request)."""
    items = "".join(
        f"<li>row-{i + 1}-{(i * 37) % 101}</li>" for i in range(BIG_ROWS)
    )
    return f"<main><h1>Big Payload</h1><ul>{items}</ul></main>"


def protected_html(authorized: bool) -> str:
    """Port of next-app/pages/protected.js ProtectedPage."""
    return (
        "<main><h1>bench-protected</h1>"
        f"<p>{'authorized' if authorized else 'unauthorized'}</p></main>"
    )


def authorized(authorization_header: str | None) -> bool:
    """Port of the TS validateAuth literal comparison."""
    return authorization_header == AUTH_HEADER
