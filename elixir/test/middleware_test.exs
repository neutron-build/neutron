defmodule Neutron.MiddlewareTest do
  use ExUnit.Case, async: true
  use Plug.Test

  describe "Neutron.Middleware.RequestId" do
    test "generates a UUID request ID" do
      conn =
        conn(:get, "/")
        |> Neutron.Middleware.RequestId.call([])

      [request_id] = get_resp_header(conn, "x-request-id")
      assert request_id =~ ~r/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
      assert conn.assigns[:request_id] == request_id
    end

    test "preserves existing request ID" do
      conn =
        conn(:get, "/")
        |> put_req_header("x-request-id", "existing-id-123")
        |> Neutron.Middleware.RequestId.call([])

      [request_id] = get_resp_header(conn, "x-request-id")
      assert request_id == "existing-id-123"
    end
  end

  describe "Neutron.Middleware.Cors" do
    test "sets CORS headers when origin is present" do
      conn =
        conn(:get, "/")
        |> put_req_header("origin", "http://localhost:3000")
        |> Neutron.Middleware.Cors.call([])

      [origin] = get_resp_header(conn, "access-control-allow-origin")
      assert origin == "http://localhost:3000"
    end

    test "handles preflight OPTIONS request" do
      conn =
        conn(:options, "/")
        |> put_req_header("origin", "http://localhost:3000")
        |> Neutron.Middleware.Cors.call([])

      assert conn.status == 204
      assert conn.halted
    end

    test "skips when no origin header" do
      conn =
        conn(:get, "/")
        |> Neutron.Middleware.Cors.call([])

      assert get_resp_header(conn, "access-control-allow-origin") == []
      refute conn.halted
    end
  end

  describe "Neutron.Middleware.OTel" do
    test "adds trace ID to response" do
      conn =
        conn(:get, "/test")
        |> Plug.Conn.assign(:request_id, "test-req-id")
        |> Neutron.Middleware.OTel.call([])

      [trace_id] = get_resp_header(conn, "x-trace-id")
      assert String.length(trace_id) == 32
      assert conn.assigns[:trace_id] == trace_id
    end

    test "preserves existing trace ID" do
      conn =
        conn(:get, "/test")
        |> put_req_header("x-trace-id", "existing-trace")
        |> Plug.Conn.assign(:request_id, "test-req-id")
        |> Neutron.Middleware.OTel.call([])

      [trace_id] = get_resp_header(conn, "x-trace-id")
      assert trace_id == "existing-trace"
    end
  end
end
