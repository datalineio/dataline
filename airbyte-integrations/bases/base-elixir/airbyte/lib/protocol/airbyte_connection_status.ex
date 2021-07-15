defmodule Airbyte.Protocol.AirbyteConnectionStatus do
  @moduledoc """
  Airbyte connection status
  https://github.com/airbytehq/airbyte/blob/master/airbyte-protocol/models/src/main/resources/airbyte_protocol/airbyte_protocol.yaml
  """
  use TypedStruct

  @derive Jason.Encoder

  typedstruct do
    @typedoc "Airbyte connection status"

    field(:status, String.t())
    field(:message, String.t())
  end

  def succeeded(message \\ nil),
    do: %__MODULE__{status: "SUCCEEDED", message: message}

  def failed(message \\ nil),
    do: %__MODULE__{status: "FAILED", message: message}
end
