defmodule Airbyte.Source.GoogleAnalytics.Source do
  @behaviour Airbyte.Source

  alias Airbyte.Protocol.{
    AirbyteCatalog,
    AirbyteConnectionStatus,
    ConfiguredAirbyteCatalog,
    ConnectorSpecification
  }

  alias Airbyte.Source.GoogleAnalytics.{Client, ConnectionSpecification, State, Streams}
  alias GoogleApi.Analytics.V3.{Api, Model}

  @streams [
    Streams.Accounts,
    Streams.Profiles,
    Streams.WebProperties
  ]

  @reports [
    "priv/reports/audience-overview.json" |> Streams.Reports.from_file(),
    "priv/reports/audience-geo.json" |> Streams.Reports.from_file(),
    "priv/reports/audience-technology.json" |> Streams.Reports.from_file()
  ]

  @connection_spec "priv/connection_specification.json"
                   |> Path.absname()
                   |> File.read!()
                   |> Jason.decode!()

  @impl Airbyte.Source
  def check(%ConnectionSpecification{} = spec) do
    with {:ok, conn} <- Client.connection(spec),
         {:ok, %Model.AccountSummaries{username: user}} <-
           Api.Management.analytics_management_account_summaries_list(conn, "max-results": 1) do
      {:ok, AirbyteConnectionStatus.succeeded("Authenticated as #{user}")}
    else
      error -> {:error, error |> Client.get_error_message()}
    end
  end

  @impl Airbyte.Source
  def discover(%ConnectionSpecification{} = spec) do
    with {:ok, conn} <- Client.connection(spec) do
      fields = Client.fields(conn)

      streams = [
        @streams |> Enum.map(& &1.stream()),
        @reports |> Enum.map(&Streams.Reports.stream(&1, fields)),
        spec |> reports() |> Enum.map(&Streams.Reports.stream(&1, fields))
      ]

      {:ok, AirbyteCatalog.create(streams |> List.flatten())}
    end
  end

  @impl Airbyte.Source
  def read(
        config: %ConnectionSpecification{} = spec,
        catalog: %ConfiguredAirbyteCatalog{} = _catalog,
        state: %State{} = state
      ) do
    new_state = State.from(spec, state)
    reports = @reports ++ reports(spec)

    streams = [
      @streams |> Stream.flat_map(& &1.read(spec)),
      reports |> Stream.flat_map(&Streams.Reports.read(spec, new_state, &1)),
      [new_state |> State.to_airbyte_state_message()]
    ]

    {:ok, Stream.flat_map(streams, &Function.identity/1)}
  end

  @impl Airbyte.Source
  def spec() do
    {:ok,
     %ConnectorSpecification{
       documentationUrl:
         "https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-google-analytics/README.md",
       changelogUrl:
         "https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-google-analytics/CHANGELOG.md",
       connectionSpecification: @connection_spec,
       supportsIncremental: true
     }}
  end

  @impl Airbyte.Source
  def connection_specification_struct(), do: ConnectionSpecification

  @impl Airbyte.Source
  def state_struct(), do: State

  defp reports(spec) when is_binary(spec.reports) do
    case Jason.decode(spec.reports) do
      {:ok, reports} -> reports
      _ -> []
    end
  end

  defp reports(_), do: []
end
