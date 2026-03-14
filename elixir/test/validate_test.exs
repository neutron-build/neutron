defmodule Neutron.ValidateTest do
  use ExUnit.Case, async: true

  alias Neutron.Validate

  describe "new/2" do
    test "filters to permitted fields only" do
      data = %{"name" => "Alice", "email" => "a@b.c", "secret" => "hidden"}
      v = Validate.new(data, [:name, :email])
      assert v.data == %{"name" => "Alice", "email" => "a@b.c"}
      assert v.valid?
    end

    test "works with atom keys in data" do
      data = %{name: "Alice", email: "a@b.c"}
      v = Validate.new(data, [:name, :email])
      assert v.valid?
    end
  end

  describe "required/2" do
    test "passes when fields are present" do
      v =
        %{"name" => "Alice", "email" => "a@b.c"}
        |> Validate.new([:name, :email])
        |> Validate.required([:name, :email])

      assert v.valid?
    end

    test "fails when fields are missing" do
      {:error, errors} =
        %{"name" => "Alice"}
        |> Validate.new([:name, :email])
        |> Validate.required([:name, :email])
        |> Validate.run()

      assert length(errors) == 1
      assert hd(errors).field == "email"
      assert hd(errors).message == "is required"
    end

    test "fails when fields are empty strings" do
      {:error, errors} =
        %{"name" => ""}
        |> Validate.new([:name])
        |> Validate.required([:name])
        |> Validate.run()

      assert length(errors) == 1
      assert hd(errors).field == "name"
    end
  end

  describe "format/3" do
    test "passes on valid format" do
      {:ok, _} =
        %{"email" => "user@example.com"}
        |> Validate.new([:email])
        |> Validate.format(:email, ~r/@/)
        |> Validate.run()
    end

    test "fails on invalid format" do
      {:error, errors} =
        %{"email" => "not-an-email"}
        |> Validate.new([:email])
        |> Validate.format(:email, ~r/@/)
        |> Validate.run()

      assert length(errors) == 1
      assert hd(errors).field == "email"
    end

    test "skips nil values" do
      {:ok, _} =
        %{}
        |> Validate.new([:email])
        |> Validate.format(:email, ~r/@/)
        |> Validate.run()
    end
  end

  describe "length/3" do
    test "min length validation" do
      {:error, errors} =
        %{"name" => "AB"}
        |> Validate.new([:name])
        |> Validate.length(:name, min: 3)
        |> Validate.run()

      assert hd(errors).message =~ "at least 3"
    end

    test "max length validation" do
      {:error, errors} =
        %{"name" => "A very long name indeed"}
        |> Validate.new([:name])
        |> Validate.length(:name, max: 10)
        |> Validate.run()

      assert hd(errors).message =~ "at most 10"
    end

    test "exact length validation" do
      {:error, errors} =
        %{"code" => "ABC"}
        |> Validate.new([:code])
        |> Validate.length(:code, is: 5)
        |> Validate.run()

      assert hd(errors).message =~ "exactly 5"
    end
  end

  describe "number/3" do
    test "greater_than validation" do
      {:error, errors} =
        %{"age" => 0}
        |> Validate.new([:age])
        |> Validate.number(:age, greater_than: 0)
        |> Validate.run()

      assert hd(errors).message =~ "greater than 0"
    end

    test "less_than validation" do
      {:error, errors} =
        %{"age" => 200}
        |> Validate.new([:age])
        |> Validate.number(:age, less_than: 150)
        |> Validate.run()

      assert hd(errors).message =~ "less than 150"
    end

    test "passes valid numbers" do
      {:ok, _} =
        %{"age" => 25}
        |> Validate.new([:age])
        |> Validate.number(:age, greater_than: 0, less_than: 150)
        |> Validate.run()
    end

    test "parses string numbers" do
      {:ok, data} =
        %{"age" => "25"}
        |> Validate.new([:age])
        |> Validate.number(:age, greater_than: 0)
        |> Validate.run()

      assert data["age"] == 25.0
    end
  end

  describe "inclusion/3" do
    test "passes when value is in list" do
      {:ok, _} =
        %{"role" => "admin"}
        |> Validate.new([:role])
        |> Validate.inclusion(:role, ["admin", "user", "moderator"])
        |> Validate.run()
    end

    test "fails when value is not in list" do
      {:error, errors} =
        %{"role" => "superadmin"}
        |> Validate.new([:role])
        |> Validate.inclusion(:role, ["admin", "user"])
        |> Validate.run()

      assert hd(errors).message =~ "must be one of"
    end
  end

  describe "custom/4" do
    test "custom validator passes" do
      {:ok, _} =
        %{"code" => "ABC123"}
        |> Validate.new([:code])
        |> Validate.custom(:code, &String.starts_with?(&1, "ABC"), "must start with ABC")
        |> Validate.run()
    end

    test "custom validator fails" do
      {:error, errors} =
        %{"code" => "XYZ123"}
        |> Validate.new([:code])
        |> Validate.custom(:code, &String.starts_with?(&1, "ABC"), "must start with ABC")
        |> Validate.run()

      assert hd(errors).message == "must start with ABC"
    end
  end

  describe "pipeline composition" do
    test "multiple validations compose" do
      {:error, errors} =
        %{"name" => "", "email" => "bad", "age" => -1}
        |> Validate.new([:name, :email, :age])
        |> Validate.required([:name, :email])
        |> Validate.format(:email, ~r/@/)
        |> Validate.number(:age, greater_than: 0)
        |> Validate.run()

      # name required, email format, age > 0
      assert length(errors) == 3
    end
  end
end
