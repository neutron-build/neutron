defmodule Nucleus.Models.KV do
  @moduledoc """
  Key-Value model — Redis-compatible operations over Nucleus SQL functions.

  Provides base KV operations plus Lists, Hashes, Sets, Sorted Sets,
  and HyperLogLog, all executed as SQL function calls over pgwire.

  ## Example

      alias Nucleus.Models.KV

      # Base operations
      KV.set(client, "user:42", "Alice")
      {:ok, "Alice"} = KV.get(client, "user:42")
      KV.set(client, "session:abc", "data", ttl: 3600)

      # Hash operations
      KV.hset(client, "user:42", "name", "Alice")
      {:ok, "Alice"} = KV.hget(client, "user:42", "name")

      # List operations
      KV.lpush(client, "queue", "job1")
      KV.rpush(client, "queue", "job2")
      {:ok, "job1"} = KV.lpop(client, "queue")
  """

  @type client :: Nucleus.Client.t()

  # --- Base Operations ---

  @doc "Gets a value by key. Returns `{:ok, value}` or `{:ok, nil}` if not found."
  @spec get(client(), String.t()) :: {:ok, String.t() | nil} | {:error, term()}
  def get(client, key) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.get") do
      case Nucleus.Client.query(client, "SELECT KV_GET($1)", [key]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:ok, %{rows: []}} -> {:ok, nil}
        {:error, _} = error -> error
      end
    end
  end

  @doc """
  Sets a key to a value.

  ## Options

    * `:ttl` — time-to-live in seconds
  """
  @spec set(client(), String.t(), String.t(), keyword()) :: :ok | {:error, term()}
  def set(client, key, value, opts \\ []) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.set") do
      ttl = Keyword.get(opts, :ttl)

      result =
        if ttl do
          Nucleus.Client.query(client, "SELECT KV_SET($1, $2, $3)", [key, value, ttl])
        else
          Nucleus.Client.query(client, "SELECT KV_SET($1, $2)", [key, value])
        end

      case result do
        {:ok, _} -> :ok
        {:error, _} = error -> error
      end
    end
  end

  @doc "Sets a key only if it does not already exist. Returns `{:ok, true}` if set."
  @spec setnx(client(), String.t(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def setnx(client, key, value) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.setnx") do
      case Nucleus.Client.query(client, "SELECT KV_SETNX($1, $2)", [key, value]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Deletes a key. Returns `{:ok, true}` if the key existed."
  @spec del(client(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def del(client, key) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.del") do
      case Nucleus.Client.query(client, "SELECT KV_DEL($1)", [key]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Checks if a key exists."
  @spec exists?(client(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def exists?(client, key) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.exists") do
      case Nucleus.Client.query(client, "SELECT KV_EXISTS($1)", [key]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Atomically increments a key's integer value. Returns the new value."
  @spec incr(client(), String.t(), integer()) :: {:ok, integer()} | {:error, term()}
  def incr(client, key, amount \\ 1) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.incr") do
      result =
        if amount == 1 do
          Nucleus.Client.query(client, "SELECT KV_INCR($1)", [key])
        else
          Nucleus.Client.query(client, "SELECT KV_INCR($1, $2)", [key, amount])
        end

      case result do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the remaining TTL in seconds. -1 = no TTL, -2 = key missing."
  @spec ttl(client(), String.t()) :: {:ok, integer()} | {:error, term()}
  def ttl(client, key) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.ttl") do
      case Nucleus.Client.query(client, "SELECT KV_TTL($1)", [key]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Sets a TTL on an existing key."
  @spec expire(client(), String.t(), integer()) :: {:ok, boolean()} | {:error, term()}
  def expire(client, key, ttl_secs) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.expire") do
      case Nucleus.Client.query(client, "SELECT KV_EXPIRE($1, $2)", [key, ttl_secs]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the total number of keys."
  @spec dbsize(client()) :: {:ok, integer()} | {:error, term()}
  def dbsize(client) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.dbsize") do
      case Nucleus.Client.query(client, "SELECT KV_DBSIZE()", []) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Deletes all keys."
  @spec flushdb(client()) :: :ok | {:error, term()}
  def flushdb(client) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.flushdb") do
      case Nucleus.Client.query(client, "SELECT KV_FLUSHDB()", []) do
        {:ok, _} -> :ok
        {:error, _} = error -> error
      end
    end
  end

  # --- List Operations ---

  @doc "Prepends a value to a list. Returns the new list length."
  @spec lpush(client(), String.t(), String.t()) :: {:ok, integer()} | {:error, term()}
  def lpush(client, key, value) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.lpush") do
      case Nucleus.Client.query(client, "SELECT KV_LPUSH($1, $2)", [key, value]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Appends a value to a list. Returns the new list length."
  @spec rpush(client(), String.t(), String.t()) :: {:ok, integer()} | {:error, term()}
  def rpush(client, key, value) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.rpush") do
      case Nucleus.Client.query(client, "SELECT KV_RPUSH($1, $2)", [key, value]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Removes and returns the first element of a list."
  @spec lpop(client(), String.t()) :: {:ok, String.t() | nil} | {:error, term()}
  def lpop(client, key) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.lpop") do
      case Nucleus.Client.query(client, "SELECT KV_LPOP($1)", [key]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:ok, %{rows: []}} -> {:ok, nil}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Removes and returns the last element of a list."
  @spec rpop(client(), String.t()) :: {:ok, String.t() | nil} | {:error, term()}
  def rpop(client, key) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.rpop") do
      case Nucleus.Client.query(client, "SELECT KV_RPOP($1)", [key]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:ok, %{rows: []}} -> {:ok, nil}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns elements from a list between start and stop (inclusive)."
  @spec lrange(client(), String.t(), integer(), integer()) ::
          {:ok, [String.t()]} | {:error, term()}
  def lrange(client, key, start, stop) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.lrange") do
      case Nucleus.Client.query(client, "SELECT KV_LRANGE($1, $2, $3)", [key, start, stop]) do
        {:ok, %{rows: [[raw]]}} when is_binary(raw) and raw != "" ->
          {:ok, String.split(raw, ",")}

        {:ok, _} ->
          {:ok, []}

        {:error, _} = error ->
          error
      end
    end
  end

  @doc "Returns the length of a list."
  @spec llen(client(), String.t()) :: {:ok, integer()} | {:error, term()}
  def llen(client, key) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.llen") do
      case Nucleus.Client.query(client, "SELECT KV_LLEN($1)", [key]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the element at the given index in a list."
  @spec lindex(client(), String.t(), integer()) :: {:ok, String.t() | nil} | {:error, term()}
  def lindex(client, key, index) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.lindex") do
      case Nucleus.Client.query(client, "SELECT KV_LINDEX($1, $2)", [key, index]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:ok, %{rows: []}} -> {:ok, nil}
        {:error, _} = error -> error
      end
    end
  end

  # --- Hash Operations ---

  @doc "Sets a field in a hash."
  @spec hset(client(), String.t(), String.t(), String.t()) ::
          {:ok, boolean()} | {:error, term()}
  def hset(client, key, field, value) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.hset") do
      case Nucleus.Client.query(client, "SELECT KV_HSET($1, $2, $3)", [key, field, value]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Gets a field value from a hash."
  @spec hget(client(), String.t(), String.t()) :: {:ok, String.t() | nil} | {:error, term()}
  def hget(client, key, field) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.hget") do
      case Nucleus.Client.query(client, "SELECT KV_HGET($1, $2)", [key, field]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:ok, %{rows: []}} -> {:ok, nil}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Deletes a field from a hash."
  @spec hdel(client(), String.t(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def hdel(client, key, field) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.hdel") do
      case Nucleus.Client.query(client, "SELECT KV_HDEL($1, $2)", [key, field]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Checks if a field exists in a hash."
  @spec hexists?(client(), String.t(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def hexists?(client, key, field) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.hexists") do
      case Nucleus.Client.query(client, "SELECT KV_HEXISTS($1, $2)", [key, field]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns all fields and values from a hash."
  @spec hgetall(client(), String.t()) :: {:ok, map()} | {:error, term()}
  def hgetall(client, key) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.hgetall") do
      case Nucleus.Client.query(client, "SELECT KV_HGETALL($1)", [key]) do
        {:ok, %{rows: [[raw]]}} when is_binary(raw) and raw != "" ->
          result =
            raw
            |> String.split(",")
            |> Enum.reduce(%{}, fn pair, acc ->
              case String.split(pair, "=", parts: 2) do
                [k, v] -> Map.put(acc, k, v)
                _ -> acc
              end
            end)

          {:ok, result}

        {:ok, _} ->
          {:ok, %{}}

        {:error, _} = error ->
          error
      end
    end
  end

  @doc "Returns the number of fields in a hash."
  @spec hlen(client(), String.t()) :: {:ok, integer()} | {:error, term()}
  def hlen(client, key) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.hlen") do
      case Nucleus.Client.query(client, "SELECT KV_HLEN($1)", [key]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  # --- Set Operations ---

  @doc "Adds a member to a set."
  @spec sadd(client(), String.t(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def sadd(client, key, member) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.sadd") do
      case Nucleus.Client.query(client, "SELECT KV_SADD($1, $2)", [key, member]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Removes a member from a set."
  @spec srem(client(), String.t(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def srem(client, key, member) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.srem") do
      case Nucleus.Client.query(client, "SELECT KV_SREM($1, $2)", [key, member]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns all members of a set."
  @spec smembers(client(), String.t()) :: {:ok, [String.t()]} | {:error, term()}
  def smembers(client, key) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.smembers") do
      case Nucleus.Client.query(client, "SELECT KV_SMEMBERS($1)", [key]) do
        {:ok, %{rows: [[raw]]}} when is_binary(raw) and raw != "" ->
          {:ok, String.split(raw, ",")}

        {:ok, _} ->
          {:ok, []}

        {:error, _} = error ->
          error
      end
    end
  end

  @doc "Checks if a member exists in a set."
  @spec sismember?(client(), String.t(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def sismember?(client, key, member) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.sismember") do
      case Nucleus.Client.query(client, "SELECT KV_SISMEMBER($1, $2)", [key, member]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the cardinality (count) of a set."
  @spec scard(client(), String.t()) :: {:ok, integer()} | {:error, term()}
  def scard(client, key) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.scard") do
      case Nucleus.Client.query(client, "SELECT KV_SCARD($1)", [key]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  # --- Sorted Set Operations ---

  @doc "Adds a member with a score to a sorted set."
  @spec zadd(client(), String.t(), float(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def zadd(client, key, score, member) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.zadd") do
      case Nucleus.Client.query(client, "SELECT KV_ZADD($1, $2, $3)", [key, score, member]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns members in a sorted set between start and stop ranks."
  @spec zrange(client(), String.t(), integer(), integer()) ::
          {:ok, [String.t()]} | {:error, term()}
  def zrange(client, key, start, stop) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.zrange") do
      case Nucleus.Client.query(client, "SELECT KV_ZRANGE($1, $2, $3)", [key, start, stop]) do
        {:ok, %{rows: [[raw]]}} when is_binary(raw) and raw != "" ->
          {:ok, String.split(raw, ",")}

        {:ok, _} ->
          {:ok, []}

        {:error, _} = error ->
          error
      end
    end
  end

  @doc "Returns members with scores between min and max."
  @spec zrangebyscore(client(), String.t(), float(), float()) ::
          {:ok, [String.t()]} | {:error, term()}
  def zrangebyscore(client, key, min, max) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.zrangebyscore") do
      case Nucleus.Client.query(client, "SELECT KV_ZRANGEBYSCORE($1, $2, $3)", [key, min, max]) do
        {:ok, %{rows: [[raw]]}} when is_binary(raw) and raw != "" ->
          {:ok, String.split(raw, ",")}

        {:ok, _} ->
          {:ok, []}

        {:error, _} = error ->
          error
      end
    end
  end

  @doc "Removes a member from a sorted set."
  @spec zrem(client(), String.t(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def zrem(client, key, member) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.zrem") do
      case Nucleus.Client.query(client, "SELECT KV_ZREM($1, $2)", [key, member]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the cardinality of a sorted set."
  @spec zcard(client(), String.t()) :: {:ok, integer()} | {:error, term()}
  def zcard(client, key) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.zcard") do
      case Nucleus.Client.query(client, "SELECT KV_ZCARD($1)", [key]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  # --- HyperLogLog ---

  @doc "Adds an element to a HyperLogLog."
  @spec pfadd(client(), String.t(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def pfadd(client, key, element) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.pfadd") do
      case Nucleus.Client.query(client, "SELECT KV_PFADD($1, $2)", [key, element]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the approximate cardinality of a HyperLogLog."
  @spec pfcount(client(), String.t()) :: {:ok, integer()} | {:error, term()}
  def pfcount(client, key) do
    with :ok <- Nucleus.Client.require_nucleus(client, "KV.pfcount") do
      case Nucleus.Client.query(client, "SELECT KV_PFCOUNT($1)", [key]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end
end
