defmodule Neutron.ConfigTest do
  use ExUnit.Case

  alias Neutron.Config

  # Clean process dictionary between tests since Config.get caches there
  setup do
    Process.delete(:neutron_config)
    :ok
  end

  describe "load/0" do
    test "returns a Config struct" do
      config = Config.load()
      assert %Config{} = config
    end

    test "has default host 0.0.0.0" do
      config = Config.load()
      assert config.host == "0.0.0.0"
    end

    test "has default port 4000" do
      config = Config.load()
      assert config.port == 4000
    end

    test "has nil database_url by default" do
      config = Config.load()
      assert config.database_url == nil
    end

    test "has default log_level :info" do
      config = Config.load()
      assert config.log_level == :info
    end

    test "has default log_format :json" do
      config = Config.load()
      assert config.log_format == :json
    end

    test "has a generated secret_key" do
      config = Config.load()
      assert is_binary(config.secret_key)
      assert String.length(config.secret_key) > 0
    end

    test "has default cors_origins ['*']" do
      config = Config.load()
      assert config.cors_origins == ["*"]
    end

    test "has default shutdown_timeout 30000" do
      config = Config.load()
      assert config.shutdown_timeout == 30_000
    end

    test "reads NEUTRON_HOST from env" do
      System.put_env("NEUTRON_HOST", "127.0.0.1")
      config = Config.load()
      assert config.host == "127.0.0.1"
      System.delete_env("NEUTRON_HOST")
    end

    test "reads NEUTRON_PORT from env" do
      System.put_env("NEUTRON_PORT", "8080")
      config = Config.load()
      assert config.port == 8080
      System.delete_env("NEUTRON_PORT")
    end

    test "reads NEUTRON_DATABASE_URL from env" do
      System.put_env("NEUTRON_DATABASE_URL", "postgres://localhost/testdb")
      config = Config.load()
      assert config.database_url == "postgres://localhost/testdb"
      System.delete_env("NEUTRON_DATABASE_URL")
    end

    test "reads NEUTRON_LOG_LEVEL from env" do
      System.put_env("NEUTRON_LOG_LEVEL", "debug")
      config = Config.load()
      assert config.log_level == :debug
      System.delete_env("NEUTRON_LOG_LEVEL")
    end

    test "reads NEUTRON_LOG_FORMAT from env" do
      System.put_env("NEUTRON_LOG_FORMAT", "text")
      config = Config.load()
      assert config.log_format == :text
      System.delete_env("NEUTRON_LOG_FORMAT")
    end

    test "defaults log_format to :json for unknown values" do
      System.put_env("NEUTRON_LOG_FORMAT", "yaml")
      config = Config.load()
      assert config.log_format == :json
      System.delete_env("NEUTRON_LOG_FORMAT")
    end

    test "reads NEUTRON_SECRET_KEY from env" do
      System.put_env("NEUTRON_SECRET_KEY", "my-secret-key-for-testing")
      config = Config.load()
      assert config.secret_key == "my-secret-key-for-testing"
      System.delete_env("NEUTRON_SECRET_KEY")
    end

    test "reads NEUTRON_CORS_ORIGINS from env (comma-separated)" do
      System.put_env("NEUTRON_CORS_ORIGINS", "http://localhost:3000, http://example.com")
      config = Config.load()
      assert config.cors_origins == ["http://localhost:3000", "http://example.com"]
      System.delete_env("NEUTRON_CORS_ORIGINS")
    end

    test "reads NEUTRON_SHUTDOWN_TIMEOUT from env" do
      System.put_env("NEUTRON_SHUTDOWN_TIMEOUT", "5000")
      config = Config.load()
      assert config.shutdown_timeout == 5000
      System.delete_env("NEUTRON_SHUTDOWN_TIMEOUT")
    end
  end

  describe "get/0" do
    test "returns the same config on subsequent calls (caches in process dict)" do
      config1 = Config.get()
      config2 = Config.get()
      assert config1 == config2
    end

    test "returns a Config struct" do
      config = Config.get()
      assert %Config{} = config
    end
  end
end
