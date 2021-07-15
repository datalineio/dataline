defmodule Airbyte.Protocol.AirbyteMessage do
  @moduledoc """
  Specification of an AirbyteMessage
  https://github.com/airbytehq/airbyte/blob/master/airbyte-protocol/models/src/main/resources/airbyte_protocol/airbyte_protocol.yaml
  """
  use TypedStruct

  alias Airbyte.Protocol.{
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteLogMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    ConnectorSpecification
  }

  @derive Jason.Encoder

  typedstruct do
    @typedoc "Specification of an AirbyteMessage"

    field(:type, String.t(), enforce: true)
    # TODO: to define struct
    field(:log, any())
    field(:spec, ConnectorSpecification)

    # TODO: to define structs
    field(:connectionStatus, any())
    field(:catalog, any())
    field(:record, any())
    field(:state, any())
  end

  def dispatch(message), do: message |> new() |> print()

  defp new(%AirbyteCatalog{} = catalog),
    do: %__MODULE__{type: "CATALOG", catalog: catalog}

  defp new(%AirbyteConnectionStatus{} = status),
    do: %__MODULE__{type: "CONNECTION_STATUS", connectionStatus: status}

  defp new(%AirbyteLogMessage{} = log),
    do: %__MODULE__{type: "LOG", log: log}

  defp new(%AirbyteRecordMessage{} = spec),
    do: %__MODULE__{type: "RECORD", record: spec}

  defp new(%AirbyteStateMessage{} = spec),
    do: %__MODULE__{type: "STATE", state: spec}

  defp new(%ConnectorSpecification{} = spec),
    do: %__MODULE__{type: "SPEC", spec: spec}

  defp print(%__MODULE__{} = message), do: message |> Jason.encode!() |> IO.puts()
end
