"""Tests for RFC 7807 error handling."""

from neutron.error import (
    AppError,
    ValidationErrorDetail,
    bad_request,
    conflict,
    forbidden,
    internal_error,
    not_found,
    rate_limited,
    unauthorized,
    validation_error,
)


def test_app_error_basic():
    err = AppError(404, "not-found", "Not Found", "User 42 not found")
    assert err.status == 404
    assert err.code == "not-found"
    assert err.title == "Not Found"
    assert err.detail == "User 42 not found"
    assert str(err) == "User 42 not found"


def test_app_error_to_response():
    err = not_found("User 42 not found")
    resp = err.to_response(instance="/api/users/42")
    assert resp.status_code == 404
    assert resp.media_type == "application/problem+json"

    import json

    body = json.loads(resp.body)
    assert body["type"] == "https://neutron.dev/errors/not-found"
    assert body["title"] == "Not Found"
    assert body["status"] == 404
    assert body["detail"] == "User 42 not found"
    assert body["instance"] == "/api/users/42"


def test_validation_error_with_details():
    errors = [
        ValidationErrorDetail(field="email", message="invalid email", value="bad"),
        ValidationErrorDetail(field="name", message="is required"),
    ]
    err = validation_error("Request body failed validation", errors)
    resp = err.to_response()

    import json

    body = json.loads(resp.body)
    assert body["status"] == 422
    assert body["type"] == "https://neutron.dev/errors/validation"
    assert len(body["errors"]) == 2
    assert body["errors"][0]["field"] == "email"
    assert body["errors"][1]["field"] == "name"
    # value=None should be excluded
    assert "value" not in body["errors"][1]


def test_convenience_constructors():
    cases = [
        (bad_request, 400, "bad-request"),
        (unauthorized, 401, "unauthorized"),
        (forbidden, 403, "forbidden"),
        (not_found, 404, "not-found"),
        (conflict, 409, "conflict"),
        (rate_limited, 429, "rate-limited"),
        (internal_error, 500, "internal"),
    ]
    for factory, expected_status, expected_code in cases:
        err = factory("test detail")
        assert err.status == expected_status
        assert err.code == expected_code
        assert err.detail == "test detail"
        assert isinstance(err, AppError)
        assert isinstance(err, Exception)
