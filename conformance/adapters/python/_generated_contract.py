"""GENERATED FROM conformance/contract-ir.json — DO NOT EDIT BY HAND.

Regenerate with:  node conformance/generate-app.mjs python --write

Plan step S43: the contract surface an SDK must expose is derived from the
machine-readable contract rather than transcribed into each language. The
error taxonomy below is not typed here — it comes from the IR, which
validate-ir.mjs keeps in agreement with FRAMEWORK_CONTRACT.md. Editing this
file by hand reintroduces exactly the drift the IR exists to prevent, and CI
runs `--check` to catch it.
"""

# (status, code, title, probe_path) for every error in FRAMEWORK_CONTRACT §2.
STANDARD_ERRORS = [
    (400, "bad-request", "Bad Request", "/errors/bad-request"),
    (401, "unauthorized", "Unauthorized", "/errors/unauthorized"),
    (403, "forbidden", "Forbidden", "/errors/forbidden"),
    (404, "not-found", "Not Found", "/errors/not-found"),
    (409, "conflict", "Conflict", "/errors/conflict"),
    (422, "validation", "Validation Failed", None),
    (429, "rate-limited", "Rate Limited", "/errors/rate-limited"),
    (500, "internal", "Internal Server Error", "/errors/internal"),
]

TYPE_BASE_URL = "https://neutron.dev/errors/"
PROBLEM_CONTENT_TYPE = "application/problem+json"
ERROR_REQUIRED_FIELDS = ["type","title","status","detail"]

# §7 health.
HEALTH_PATH = "/health"
HEALTH_KEYS = ["status","nucleus","version"]
NUCLEUS_STATES = ["connected","disconnected","unconfigured"]

# §2 validation.
VALIDATION_ENDPOINT = "/api/items"
VALIDATION_STATUS = 422
VALIDATION_ERRORS_FIELD = "errors"

# §4 OpenAPI.
OPENAPI_SPEC_PATH = "/openapi.json"
OPENAPI_VERSION_PREFIX = "3.1"


def error_type_url(code: str) -> str:
    """The RFC 7807 `type` URI for a standard error code."""
    return TYPE_BASE_URL + code


def probed_errors():
    """The errors with a forced-error endpoint.

    `validation` is excluded on purpose: it has no GET probe because it is
    produced by POSTing an invalid body, and is asserted by the
    `validation.format` dimension instead.
    """
    return [e for e in STANDARD_ERRORS if e[3] is not None]
