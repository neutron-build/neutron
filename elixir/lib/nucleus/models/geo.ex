defmodule Nucleus.Models.Geo do
  @moduledoc """
  Geospatial model — GEO_DISTANCE, GEO_WITHIN, ST_MAKEPOINT, etc.

  ## Example

      alias Nucleus.Models.Geo

      {:ok, meters} = Geo.distance(client, 40.7128, -74.0060, 51.5074, -0.1278)
      {:ok, true} = Geo.within?(client, 40.7128, -74.0060, 40.7138, -74.0050, 1000.0)
  """

  @type client :: Nucleus.Client.t()

  @doc "Calculates the haversine distance between two points in meters."
  @spec distance(client(), float(), float(), float(), float()) ::
          {:ok, float()} | {:error, term()}
  def distance(client, lat1, lon1, lat2, lon2) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Geo.distance") do
      case Nucleus.Client.query(client, "SELECT GEO_DISTANCE($1, $2, $3, $4)", [
             lat1,
             lon1,
             lat2,
             lon2
           ]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Calculates Euclidean distance between two 2D points."
  @spec distance_euclidean(client(), float(), float(), float(), float()) ::
          {:ok, float()} | {:error, term()}
  def distance_euclidean(client, x1, y1, x2, y2) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Geo.distance_euclidean") do
      case Nucleus.Client.query(client, "SELECT GEO_DISTANCE_EUCLIDEAN($1, $2, $3, $4)", [
             x1,
             y1,
             x2,
             y2
           ]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Checks if two points are within a given radius (meters)."
  @spec within?(client(), float(), float(), float(), float(), float()) ::
          {:ok, boolean()} | {:error, term()}
  def within?(client, lat1, lon1, lat2, lon2, radius_m) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Geo.within") do
      case Nucleus.Client.query(client, "SELECT GEO_WITHIN($1, $2, $3, $4, $5)", [
             lat1,
             lon1,
             lat2,
             lon2,
             radius_m
           ]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Calculates the area of a bounding box."
  @spec area(client(), float(), float(), float(), float()) ::
          {:ok, float()} | {:error, term()}
  def area(client, lon1, lat1, lon2, lat2) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Geo.area") do
      case Nucleus.Client.query(client, "SELECT GEO_AREA($1, $2, $3, $4)", [
             lon1,
             lat1,
             lon2,
             lat2
           ]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Creates a POINT type from longitude and latitude."
  @spec make_point(client(), float(), float()) :: {:ok, term()} | {:error, term()}
  def make_point(client, lon, lat) do
    case Nucleus.Client.query(client, "SELECT ST_MAKEPOINT($1, $2)", [lon, lat]) do
      {:ok, %{rows: [[val]]}} -> {:ok, val}
      {:error, _} = error -> error
    end
  end

  @doc "Extracts the X (longitude) coordinate from a POINT."
  @spec st_x(client(), term()) :: {:ok, float()} | {:error, term()}
  def st_x(client, point) do
    case Nucleus.Client.query(client, "SELECT ST_X($1)", [point]) do
      {:ok, %{rows: [[val]]}} -> {:ok, val}
      {:error, _} = error -> error
    end
  end

  @doc "Extracts the Y (latitude) coordinate from a POINT."
  @spec st_y(client(), term()) :: {:ok, float()} | {:error, term()}
  def st_y(client, point) do
    case Nucleus.Client.query(client, "SELECT ST_Y($1)", [point]) do
      {:ok, %{rows: [[val]]}} -> {:ok, val}
      {:error, _} = error -> error
    end
  end
end
