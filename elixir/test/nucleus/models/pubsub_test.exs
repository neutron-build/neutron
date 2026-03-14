defmodule Nucleus.Models.PubSubTest do
  use ExUnit.Case, async: true

  alias Nucleus.Models.PubSub

  describe "module exports" do
    test "exports publish/3" do
      assert function_exported?(PubSub, :publish, 3)
    end

    test "exports channels/1 and channels/2" do
      assert function_exported?(PubSub, :channels, 1)
      assert function_exported?(PubSub, :channels, 2)
    end

    test "exports subscribers/2" do
      assert function_exported?(PubSub, :subscribers, 2)
    end
  end
end
