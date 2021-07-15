defmodule Airbyte.Source.GoogleAnalytics.Streams.Profiles do
  @moduledoc "Profiles Stream"
  use TypedStruct

  alias Airbyte.Protocol.{AirbyteStream, AirbyteRecordMessage}
  alias Airbyte.Source.GoogleAnalytics.{Client, ConnectionSpecification}
  alias GoogleApi.Analytics.V3.{Api, Model}

  @derive Jason.Encoder

  @name "profiles"
  @schema "priv/streams/profiles.json"
          |> Path.absname()
          |> File.read!()
          |> Jason.decode!()

  typedstruct do
    field(:id, String.t(), enforce: true)
    field(:name, String.t(), enforce: true)
    field(:type, String.t(), enforce: true)
    field(:account_id, String.t(), enforce: true)
    field(:web_property_id, String.t(), enforce: true)
  end

  def stream() do
    %AirbyteStream{
      name: @name,
      json_schema: Map.put_new(@schema, :description, __MODULE__),
      supported_sync_modes: ["full_refresh"],
      source_defined_cursor: false
    }
  end

  def new(
        %Model.AccountSummary{id: account_id},
        %Model.WebPropertySummary{id: property_id},
        %Model.ProfileSummary{} = profile
      ) do
    %__MODULE__{
      id: profile.id,
      name: profile.name,
      type: profile.type,
      account_id: account_id,
      web_property_id: property_id
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
    account.webProperties |> Enum.map(&process_web_property(account, &1))
  end

  defp process_web_property(
         %Model.AccountSummary{} = account,
         %Model.WebPropertySummary{} = web_property
       ) do
    web_property.profiles
    |> Stream.map(&__MODULE__.new(account, web_property, &1))
    |> Stream.map(&__MODULE__.record/1)
    |> Enum.to_list()
  end
end
