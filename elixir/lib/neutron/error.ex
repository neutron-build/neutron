defmodule Neutron.Error do
  @moduledoc """
  RFC 7807 Problem Details for HTTP APIs.

  All Neutron frameworks return errors in the standard RFC 7807 format. This module
  provides constructors for every standard error code defined in the Framework Contract.

  ## Example

      iex> Neutron.Error.not_found("User 42 not found")
      %Neutron.Error{
        type: "https://neutron.dev/errors/not-found",
        title: "Not Found",
        status: 404,
        detail: "User 42 not found",
        instance: nil,
        errors: nil
      }
  """

  @derive Jason.Encoder
  defstruct [:type, :title, :status, :detail, :instance, :errors]

  @type validation_error :: %{
          field: String.t(),
          message: String.t(),
          value: any() | nil
        }

  @type t :: %__MODULE__{
          type: String.t(),
          title: String.t(),
          status: non_neg_integer(),
          detail: String.t(),
          instance: String.t() | nil,
          errors: [validation_error()] | nil
        }

  @base_url "https://neutron.dev/errors"

  @doc "Creates a 400 Bad Request error."
  @spec bad_request(String.t()) :: t()
  def bad_request(detail) do
    %__MODULE__{
      type: "#{@base_url}/bad-request",
      title: "Bad Request",
      status: 400,
      detail: detail
    }
  end

  @doc "Creates a 401 Unauthorized error."
  @spec unauthorized(String.t()) :: t()
  def unauthorized(detail \\ "Authentication required") do
    %__MODULE__{
      type: "#{@base_url}/unauthorized",
      title: "Unauthorized",
      status: 401,
      detail: detail
    }
  end

  @doc "Creates a 403 Forbidden error."
  @spec forbidden(String.t()) :: t()
  def forbidden(detail \\ "Access denied") do
    %__MODULE__{
      type: "#{@base_url}/forbidden",
      title: "Forbidden",
      status: 403,
      detail: detail
    }
  end

  @doc "Creates a 404 Not Found error."
  @spec not_found(String.t()) :: t()
  def not_found(detail \\ "Resource not found") do
    %__MODULE__{
      type: "#{@base_url}/not-found",
      title: "Not Found",
      status: 404,
      detail: detail
    }
  end

  @doc "Creates a 409 Conflict error."
  @spec conflict(String.t()) :: t()
  def conflict(detail) do
    %__MODULE__{
      type: "#{@base_url}/conflict",
      title: "Conflict",
      status: 409,
      detail: detail
    }
  end

  @doc "Creates a 422 Validation Failed error with field-level errors."
  @spec validation(String.t(), [validation_error()]) :: t()
  def validation(detail \\ "Request body failed validation", errors \\ []) do
    %__MODULE__{
      type: "#{@base_url}/validation",
      title: "Validation Failed",
      status: 422,
      detail: detail,
      errors: errors
    }
  end

  @doc "Creates a 429 Rate Limited error."
  @spec rate_limited(String.t()) :: t()
  def rate_limited(detail \\ "Too many requests") do
    %__MODULE__{
      type: "#{@base_url}/rate-limited",
      title: "Rate Limited",
      status: 429,
      detail: detail
    }
  end

  @doc "Creates a 500 Internal Server Error."
  @spec internal(String.t()) :: t()
  def internal(detail \\ "An unexpected error occurred") do
    %__MODULE__{
      type: "#{@base_url}/internal",
      title: "Internal Server Error",
      status: 500,
      detail: detail
    }
  end

  @doc "Creates a 501 Not Implemented error (used for Nucleus-required features on plain PG)."
  @spec nucleus_required(String.t()) :: t()
  def nucleus_required(feature) do
    %__MODULE__{
      type: "#{@base_url}/nucleus-required",
      title: "Nucleus Required",
      status: 501,
      detail: "#{feature} requires Nucleus database, but connected to plain PostgreSQL"
    }
  end

  @doc "Sets the instance path on an error."
  @spec with_instance(t(), String.t()) :: t()
  def with_instance(%__MODULE__{} = error, instance) do
    %{error | instance: instance}
  end

  @doc """
  Converts a Neutron.Error to a JSON-encodable map, stripping nil fields.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = error) do
    %{
      type: error.type,
      title: error.title,
      status: error.status,
      detail: error.detail
    }
    |> maybe_put(:instance, error.instance)
    |> maybe_put(:errors, error.errors)
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  @doc """
  Sends an RFC 7807 error response on a Plug.Conn.
  """
  @spec send_error(Plug.Conn.t(), t()) :: Plug.Conn.t()
  def send_error(conn, %__MODULE__{} = error) do
    conn
    |> Plug.Conn.put_resp_content_type("application/problem+json")
    |> Plug.Conn.send_resp(error.status, Jason.encode!(to_map(error)))
    |> Plug.Conn.halt()
  end
end

defimpl Jason.Encoder, for: Neutron.Error do
  def encode(error, opts) do
    Neutron.Error.to_map(error)
    |> Jason.Encode.map(opts)
  end
end
