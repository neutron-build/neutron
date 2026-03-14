defmodule Nucleus.Models.PubSub do
  @moduledoc """
  PubSub model — PUBSUB_PUBLISH, PUBSUB_CHANNELS, PUBSUB_SUBSCRIBERS.

  ## Example

      alias Nucleus.Models.PubSub

      {:ok, count} = PubSub.publish(client, "notifications", "Hello!")
      {:ok, channels} = PubSub.channels(client)
      {:ok, subs} = PubSub.subscribers(client, "notifications")
  """

  @type client :: Nucleus.Client.t()

  @doc "Publishes a message to a channel. Returns the number of subscribers reached."
  @spec publish(client(), String.t(), String.t()) :: {:ok, integer()} | {:error, term()}
  def publish(client, channel, message) do
    with :ok <- Nucleus.Client.require_nucleus(client, "PubSub.publish") do
      case Nucleus.Client.query(client, "SELECT PUBSUB_PUBLISH($1, $2)", [channel, message]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Lists active channels, optionally filtered by pattern."
  @spec channels(client(), String.t() | nil) :: {:ok, [String.t()]} | {:error, term()}
  def channels(client, pattern \\ nil) do
    with :ok <- Nucleus.Client.require_nucleus(client, "PubSub.channels") do
      result =
        if pattern do
          Nucleus.Client.query(client, "SELECT PUBSUB_CHANNELS($1)", [pattern])
        else
          Nucleus.Client.query(client, "SELECT PUBSUB_CHANNELS()", [])
        end

      case result do
        {:ok, %{rows: [[raw]]}} when is_binary(raw) and raw != "" ->
          {:ok, String.split(raw, ",")}

        {:ok, _} ->
          {:ok, []}

        {:error, _} = error ->
          error
      end
    end
  end

  @doc "Returns the number of subscribers for a channel."
  @spec subscribers(client(), String.t()) :: {:ok, integer()} | {:error, term()}
  def subscribers(client, channel) do
    with :ok <- Nucleus.Client.require_nucleus(client, "PubSub.subscribers") do
      case Nucleus.Client.query(client, "SELECT PUBSUB_SUBSCRIBERS($1)", [channel]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end
end
