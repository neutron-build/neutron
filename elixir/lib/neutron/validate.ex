defmodule Neutron.Validate do
  @moduledoc """
  Input validation using an Ecto.Changeset-inspired API.

  Provides a lightweight validation pipeline without requiring Ecto schemas.
  Validators compose via the pipe operator and produce RFC 7807-compatible errors.

  ## Example

      alias Neutron.Validate

      case params
           |> Validate.new([:name, :email, :age])
           |> Validate.required([:name, :email])
           |> Validate.format(:email, ~r/@/)
           |> Validate.number(:age, greater_than: 0, less_than: 150)
           |> Validate.run() do
        {:ok, validated} ->
          json(conn, 200, %{user: validated})

        {:error, errors} ->
          send_error(conn, Neutron.Error.validation("Validation failed", errors))
      end
  """

  @type field_error :: %{field: String.t(), message: String.t(), value: any()}
  @type t :: %__MODULE__{
          data: map(),
          errors: [field_error()],
          permitted: [atom() | String.t()],
          valid?: boolean()
        }

  defstruct data: %{}, errors: [], permitted: [], valid?: true

  @doc """
  Creates a new validation context from input data.

  Only keys listed in `permitted` will pass through.
  """
  @spec new(map(), [atom() | String.t()]) :: t()
  def new(data, permitted \\ []) when is_map(data) do
    permitted_strings = Enum.map(permitted, &to_string/1)

    filtered =
      data
      |> Enum.filter(fn {k, _v} -> to_string(k) in permitted_strings end)
      |> Enum.into(%{})

    %__MODULE__{data: filtered, permitted: permitted}
  end

  @doc """
  Validates that the given fields are present and non-empty.
  """
  @spec required(t(), [atom() | String.t()]) :: t()
  def required(%__MODULE__{} = v, fields) do
    Enum.reduce(fields, v, fn field, acc ->
      key = to_string(field)

      case Map.get(acc.data, key) do
        nil -> add_error(acc, key, "is required", nil)
        "" -> add_error(acc, key, "is required", "")
        _ -> acc
      end
    end)
  end

  @doc """
  Validates a field matches the given regex pattern.
  """
  @spec format(t(), atom() | String.t(), Regex.t(), String.t()) :: t()
  def format(%__MODULE__{} = v, field, pattern, message \\ "has invalid format") do
    key = to_string(field)

    case Map.get(v.data, key) do
      nil ->
        v

      val when is_binary(val) ->
        if Regex.match?(pattern, val), do: v, else: add_error(v, key, message, val)

      val ->
        add_error(v, key, message, val)
    end
  end

  @doc """
  Validates a string field's length.

  ## Options

    * `:min` — minimum length
    * `:max` — maximum length
    * `:is` — exact length
  """
  @spec length(t(), atom() | String.t(), keyword()) :: t()
  def length(%__MODULE__{} = v, field, opts) do
    key = to_string(field)

    case Map.get(v.data, key) do
      nil ->
        v

      val when is_binary(val) ->
        len = String.length(val)
        v = check_length(v, key, val, len, :min, opts)
        v = check_length(v, key, val, len, :max, opts)
        check_length(v, key, val, len, :is, opts)

      _ ->
        v
    end
  end

  @doc """
  Validates a numeric field.

  ## Options

    * `:greater_than` — value must be > n
    * `:greater_than_or_equal_to` — value must be >= n
    * `:less_than` — value must be < n
    * `:less_than_or_equal_to` — value must be <= n
    * `:equal_to` — value must be == n
  """
  @spec number(t(), atom() | String.t(), keyword()) :: t()
  def number(%__MODULE__{} = v, field, opts) do
    key = to_string(field)

    case Map.get(v.data, key) do
      nil ->
        v

      val when is_number(val) ->
        v
        |> check_number(key, val, :greater_than, opts)
        |> check_number(key, val, :greater_than_or_equal_to, opts)
        |> check_number(key, val, :less_than, opts)
        |> check_number(key, val, :less_than_or_equal_to, opts)
        |> check_number(key, val, :equal_to, opts)

      val when is_binary(val) ->
        case Float.parse(val) do
          {num, _} -> number(%{v | data: Map.put(v.data, key, num)}, field, opts)
          :error -> add_error(v, key, "must be a number", val)
        end

      val ->
        add_error(v, key, "must be a number", val)
    end
  end

  @doc """
  Validates that a field's value is in the given list.
  """
  @spec inclusion(t(), atom() | String.t(), list()) :: t()
  def inclusion(%__MODULE__{} = v, field, values) do
    key = to_string(field)

    case Map.get(v.data, key) do
      nil -> v
      val -> if val in values, do: v, else: add_error(v, key, "must be one of: #{inspect(values)}", val)
    end
  end

  @doc """
  Adds a custom validation.
  """
  @spec custom(t(), atom() | String.t(), (any() -> boolean()), String.t()) :: t()
  def custom(%__MODULE__{} = v, field, validator_fn, message \\ "is invalid") do
    key = to_string(field)

    case Map.get(v.data, key) do
      nil -> v
      val -> if validator_fn.(val), do: v, else: add_error(v, key, message, val)
    end
  end

  @doc """
  Runs the validation and returns `{:ok, data}` or `{:error, errors}`.
  """
  @spec run(t()) :: {:ok, map()} | {:error, [field_error()]}
  def run(%__MODULE__{valid?: true, data: data}), do: {:ok, data}
  def run(%__MODULE__{errors: errors}), do: {:error, Enum.reverse(errors)}

  # --- Internal ---

  defp add_error(%__MODULE__{} = v, field, message, value) do
    error = %{field: field, message: message, value: value}
    %{v | errors: [error | v.errors], valid?: false}
  end

  defp check_length(v, key, val, len, :min, opts) do
    case Keyword.get(opts, :min) do
      nil -> v
      min when len < min -> add_error(v, key, "must be at least #{min} characters", val)
      _ -> v
    end
  end

  defp check_length(v, key, val, len, :max, opts) do
    case Keyword.get(opts, :max) do
      nil -> v
      max when len > max -> add_error(v, key, "must be at most #{max} characters", val)
      _ -> v
    end
  end

  defp check_length(v, key, val, len, :is, opts) do
    case Keyword.get(opts, :is) do
      nil -> v
      is when len != is -> add_error(v, key, "must be exactly #{is} characters", val)
      _ -> v
    end
  end

  defp check_number(v, key, val, :greater_than, opts) do
    case Keyword.get(opts, :greater_than) do
      nil -> v
      n when val <= n -> add_error(v, key, "must be greater than #{n}", val)
      _ -> v
    end
  end

  defp check_number(v, key, val, :greater_than_or_equal_to, opts) do
    case Keyword.get(opts, :greater_than_or_equal_to) do
      nil -> v
      n when val < n -> add_error(v, key, "must be greater than or equal to #{n}", val)
      _ -> v
    end
  end

  defp check_number(v, key, val, :less_than, opts) do
    case Keyword.get(opts, :less_than) do
      nil -> v
      n when val >= n -> add_error(v, key, "must be less than #{n}", val)
      _ -> v
    end
  end

  defp check_number(v, key, val, :less_than_or_equal_to, opts) do
    case Keyword.get(opts, :less_than_or_equal_to) do
      nil -> v
      n when val > n -> add_error(v, key, "must be less than or equal to #{n}", val)
      _ -> v
    end
  end

  defp check_number(v, key, val, :equal_to, opts) do
    case Keyword.get(opts, :equal_to) do
      nil -> v
      n when val != n -> add_error(v, key, "must be equal to #{n}", val)
      _ -> v
    end
  end
end
