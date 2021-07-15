defmodule Airbyte.Source.GoogleAnalytics.Streams.Accounts do
  @moduledoc "Accounts Stream"
  use TypedStruct

  alias Airbyte.Protocol.{AirbyteStream, AirbyteRecordMessage}
  alias Airbyte.Source.GoogleAnalytics.{Client, ConnectionSpecification}
  alias GoogleApi.Analytics.V3.{Api, Model}

  @derive Jason.Encoder

  @name "accounts"
  @schema "priv/streams/accounts.json"
          |> Path.absname()
          |> File.read!()
          |> Jason.decode!()

  typedstruct do
    field(:id, String.t(), enforce: true)
    field(:name, String.t(), enforce: true)
  end

  def stream() do
    %AirbyteStream{
      name: @name,
      json_schema: Map.put_new(@schema, :description, __MODULE__),
      supported_sync_modes: ["full_refresh"],
      source_defined_cursor: false
    }
  end

  def new(%Model.AccountSummary{id: id, name: name}) do
    %__MODULE__{id: id, name: name}
  end

  def record(%__MODULE__{} = stream) do
    AirbyteRecordMessage.new(@name, Map.from_struct(stream))
  end

  def read(%ConnectionSpecification{} = spec) do
    with {:ok, conn} <- Client.connection(spec),
         {:ok, summary} <- Api.Management.analytics_management_account_summaries_list(conn) do
      summary.items
      |> Stream.map(&__MODULE__.new/1)
      |> Stream.map(&__MODULE__.record/1)
      |> Enum.to_list()
    end
  end
end
