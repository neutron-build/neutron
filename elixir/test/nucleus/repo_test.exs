defmodule Nucleus.RepoTest do
  use ExUnit.Case, async: true

  alias Nucleus.Repo

  describe "valid_identifier?/1" do
    test "accepts valid SQL identifiers" do
      assert Repo.valid_identifier?("users")
      assert Repo.valid_identifier?("user_profiles")
      assert Repo.valid_identifier?("_private")
      assert Repo.valid_identifier?("Table1")
      assert Repo.valid_identifier?("a")
    end

    test "rejects invalid SQL identifiers" do
      refute Repo.valid_identifier?("1users")
      refute Repo.valid_identifier?("user-profiles")
      refute Repo.valid_identifier?("user profiles")
      refute Repo.valid_identifier?("user;drop")
      refute Repo.valid_identifier?("")
      refute Repo.valid_identifier?("user.table")
    end

    test "rejects SQL injection attempts" do
      refute Repo.valid_identifier?("users; DROP TABLE users")
      refute Repo.valid_identifier?("users'--")
      refute Repo.valid_identifier?("users OR 1=1")
    end
  end

  # The following tests verify SQL generation behavior by testing that the
  # all/insert/update/delete functions raise for invalid identifiers.
  # Actual query execution requires a database connection, which we skip here.

  describe "all/3 identifier validation" do
    test "raises ArgumentError for invalid table name" do
      assert_raise ArgumentError, fn ->
        Repo.all(:fake_client, "1invalid_table")
      end
    end

    test "raises ArgumentError for SQL injection in table name" do
      assert_raise ArgumentError, fn ->
        Repo.all(:fake_client, "users; DROP TABLE users")
      end
    end
  end

  describe "insert/3 identifier validation" do
    test "raises ArgumentError for invalid table name" do
      assert_raise ArgumentError, fn ->
        Repo.insert(:fake_client, "drop--table", %{name: "test"})
      end
    end
  end

  describe "update/4 identifier validation" do
    test "raises ArgumentError for invalid table name" do
      assert_raise ArgumentError, fn ->
        Repo.update(:fake_client, "bad table!", %{name: "test"})
      end
    end
  end

  describe "delete/3 identifier validation" do
    test "raises ArgumentError for invalid table name" do
      assert_raise ArgumentError, fn ->
        Repo.delete(:fake_client, "bad;table")
      end
    end
  end
end
