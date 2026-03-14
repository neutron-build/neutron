defmodule Neutron.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/neutron-build/neutron"

  def project do
    [
      app: :neutron,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "Neutron",
      description: "Fault-tolerant, distributed web framework for the Neutron ecosystem",
      source_url: @source_url,
      docs: docs(),
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {Neutron.App, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      # HTTP server
      {:bandit, "~> 1.2"},
      {:plug, "~> 1.15"},
      {:plug_cowboy, "~> 2.7", optional: true},

      # Database
      {:postgrex, "~> 0.18"},
      {:ecto_sql, "~> 3.11"},

      # JSON
      {:jason, "~> 1.4"},

      # Auth
      {:jose, "~> 1.11"},

      # Telemetry / OTel
      {:telemetry, "~> 1.2"},
      {:opentelemetry_api, "~> 1.3", optional: true},

      # WebSocket
      {:websock, "~> 0.5"},
      {:websock_adapter, "~> 0.5"},

      # Dev / Test
      {:ex_doc, "~> 0.31", only: :dev, runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp docs do
    [
      main: "Neutron",
      extras: ["README.md"]
    ]
  end

  defp aliases do
    [
      setup: ["deps.get", "ecto.setup"],
      "ecto.setup": ["ecto.create", "ecto.migrate"],
      "ecto.reset": ["ecto.drop", "ecto.setup"],
      test: ["ecto.create --quiet", "ecto.migrate --quiet", "test"]
    ]
  end
end
