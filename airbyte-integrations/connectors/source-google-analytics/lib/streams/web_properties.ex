defmodule Airbyte.Source.GoogleAnalytics.Streams.WebProperties do
  @moduledoc "Web Properties Stream"
  use TypedStruct

  alias Airbyte.Protocol.{AirbyteStream, AirbyteRecordMessage}
  alias Airbyte.Source.GoogleAnalytics.{Client, ConnectionSpecification}
  alias GoogleApi.Analytics.V3.{Api, Model}

  @derive Jason.Encoder

  @name "web_properties"
  @schema "priv/streams/web_properties.json"
          |> Path.absname()
          |> File.read!()
          |> Jason.decode!()

  typedstruct do
    field(:id, String.t(), enforce: true)
    field(:name, String.t(), enforce: true)
    field(:level, String.t(), enforce: true)
    field(:website_url, String.t(), enforce: true)
    field(:account_id, String.t(), enforce: true)
  end

  def stream() do
    %AirbyteStream{
      name: @name,
      json_schema: Map.put_new(@schema, :description, __MODULE__),
      supported_sync_modes: ["full_refresh"],
      source_defined_cursor: false
    }
  end

  def new(%Model.AccountSummary{id: account_id}, %Model.WebPropertySummary{} = property) do
    %__MODULE__{
      id: property.id,
      name: property.name,
      level: property.level,
      website_url: property.websiteUrl,
      account_id: account_id
    }
  end

  def record(%__MODULE__{} = stream) do
    AirbyteRecordMessage.new(@name, Map.from_struct(stream))
  end

  def read(%ConnectionSpecification{} = spec) do
    with {:ok, conn} <- Client.connection(spec),
         {:ok, summary} <- Api.Management.analytics_management_account_summaries_list(conn) do
      summary.items
      |> Enum.map(&process_account/1)
      |> List.flatten()
    end
  end

  defp process_account(%Model.AccountSummary{} = account) do
    account.webProperties
    |> Stream.map(&__MODULE__.new(account, &1))
    |> Stream.map(&__MODULE__.record/1)
    |> Enum.to_list()
  end
end
