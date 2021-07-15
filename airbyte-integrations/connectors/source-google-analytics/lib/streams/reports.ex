defmodule Airbyte.Source.GoogleAnalytics.Streams.Reports do
  @moduledoc "Accounts Stream"
  use TypedStruct

  require Logger

  alias Airbyte.Protocol.{AirbyteRecordMessage, AirbyteStream}

  alias Airbyte.Source.GoogleAnalytics.{
    Client,
    ConnectionSpecification,
    DataRequest,
    Schema,
    State
  }

  alias GoogleApi.Analytics.V3.Model

  @derive Jason.Encoder

  typedstruct do
    field(:name, String.t(), enforce: true)
    field(:metrics, list(String.t()), enforce: true)
    field(:dimensions, list(String.t()), enforce: true)
    field(:cursor, String.t(), enforce: true)
  end

  def stream(report, fields) do
    %AirbyteStream{
      name: report.name,
      json_schema: schema(report, fields),
      supported_sync_modes: ["full_refresh", "incremental"],
      default_cursor_field: default_cursor_field(report),
      source_defined_cursor: is_nil(report.cursor) == false
    }
  end

  def record(%__MODULE__{} = report, data) do
    AirbyteRecordMessage.new(report.name, data)
  end

  def from_file(path) do
    Airbyte.Helpers.json_to_struct(path, __MODULE__)
  end

  def read(%ConnectionSpecification{} = spec, %State{} = state, %__MODULE__{} = report) do
    with {:ok, conn} <- Client.connection(spec),
         {:ok, profiles} <- Client.profiles(conn) do
      profiles
      |> Stream.map(&generate_request(state, report, &1))
      |> Stream.map(&DataRequest.query(conn, &1))
      |> Stream.flat_map(fn
        {:ok, data} ->
          data |> Stream.map(&record(report, &1))

        {:error, error} ->
          Logger.warn(error)
          []
      end)
    end
  end

  defp default_cursor_field(%__MODULE__{} = report) do
    case report.cursor do
      nil -> nil
      cursor -> cursor |> Schema.to_field_name()
    end
  end

  defp generate_request(
         %State{} = state,
         %__MODULE__{} = report,
         %Model.ProfileSummary{} = profile
       ) do
    %DataRequest{
      profile_id: profile.id,
      start_date: state.start_date,
      end_date: state.end_date,
      metrics: report.metrics,
      dimensions: report.dimensions
    }
  end

  defp schema(%__MODULE__{} = report, fields) do
    properties =
      (report.metrics ++ report.dimensions)
      |> Enum.map(&to_property(&1, fields))
      |> Enum.into(%{})

    %{
      "$schema": "http://json-schema.org/draft-07/schema#",
      "$id":
        "https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-google-analytics/priv/reports/#{
          report.name
        }.json",
      description: __MODULE__,
      additionalProperties: false,
      title: report.name,
      type: "object",
      properties: properties
    }
  end

  defp to_property(name, fields) do
    id = Schema.to_field_name(name)

    fields = %{
      "type" => fields[name].dataType |> Schema.to_type(),
      "description" => name
    }

    {id, fields}
  end
end
