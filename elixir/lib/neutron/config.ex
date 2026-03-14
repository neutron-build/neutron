defmodule Neutron.Config do
  @moduledoc """
  Typed configuration loaded from environment variables.

  All Neutron frameworks share the same env var contract with the `NEUTRON_` prefix.

  ## Environment Variables

  | Variable             | Description                  | Default     |
  |----------------------|------------------------------|-------------|
  | `NEUTRON_HOST`       | Server bind address          | `0.0.0.0`   |
  | `NEUTRON_PORT`       | Server port                  | `4000`      |
  | `NEUTRON_DATABASE_URL` | PostgreSQL/Nucleus URL     | `nil`       |
  | `NEUTRON_LOG_LEVEL`  | Logging level                | `info`      |
  | `NEUTRON_LOG_FORMAT` | Log format (`json` or `text`)| `json`      |
  | `NEUTRON_SECRET_KEY` | Secret key for JWT/sessions  | generated   |
  | `NEUTRON_CORS_ORIGINS` | Comma-separated origins    | `*`         |
  | `NEUTRON_SHUTDOWN_TIMEOUT` | Graceful shutdown ms   | `30000`     |
  """

  @enforce_keys []
  defstruct [
    :host,
    :port,
    :database_url,
    :log_level,
    :log_format,
    :secret_key,
    :cors_origins,
    :shutdown_timeout
  ]

  @type t :: %__MODULE__{
          host: String.t(),
          port: non_neg_integer(),
          database_url: String.t() | nil,
          log_level: atom(),
          log_format: :json | :text,
          secret_key: String.t(),
          cors_origins: [String.t()],
          shutdown_timeout: non_neg_integer()
        }

  @doc """
  Loads configuration from environment variables with defaults.
  """
  @spec load() :: t()
  def load do
    %__MODULE__{
      host: env("NEUTRON_HOST", "0.0.0.0"),
      port: env_int("NEUTRON_PORT", 4000),
      database_url: env("NEUTRON_DATABASE_URL", nil),
      log_level: env("NEUTRON_LOG_LEVEL", "info") |> String.to_atom(),
      log_format: env("NEUTRON_LOG_FORMAT", "json") |> parse_log_format(),
      secret_key: env("NEUTRON_SECRET_KEY", generate_secret()),
      cors_origins: env("NEUTRON_CORS_ORIGINS", "*") |> parse_origins(),
      shutdown_timeout: env_int("NEUTRON_SHUTDOWN_TIMEOUT", 30_000)
    }
  end

  @doc """
  Returns the current configuration, loading from env if needed.
  """
  @spec get() :: t()
  def get do
    case Process.get(:neutron_config) do
      nil ->
        config = load()
        Process.put(:neutron_config, config)
        config

      config ->
        config
    end
  end

  defp env(key, default) do
    System.get_env(key) || default
  end

  defp env_int(key, default) do
    case System.get_env(key) do
      nil -> default
      val -> String.to_integer(val)
    end
  end

  defp parse_log_format("text"), do: :text
  defp parse_log_format(_), do: :json

  defp parse_origins("*"), do: ["*"]

  defp parse_origins(origins) when is_binary(origins) do
    origins
    |> String.split(",", trim: true)
    |> Enum.map(&String.trim/1)
  end

  defp generate_secret do
    :crypto.strong_rand_bytes(32) |> Base.url_encode64(padding: false)
  end
end
