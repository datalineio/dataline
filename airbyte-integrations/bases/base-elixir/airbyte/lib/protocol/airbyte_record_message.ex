defmodule Airbyte.Protocol.AirbyteRecordMessage do
  @moduledoc """
  Specification of an AirbyteRecordMessage
  https://github.com/airbytehq/airbyte/blob/master/airbyte-protocol/models/src/main/resources/airbyte_protocol/airbyte_protocol.yaml
  """
  use TypedStruct

  @derive Jason.Encoder

  typedstruct do
    @typedoc "Specification of an AirbyteRecordMessage"

    field(:stream, String.t(), enforce: true)
    field(:data, any(), enforce: true)
    field(:emitted_at, integer, enforce: true)
  end

  def new(stream, data) do
    %__MODULE__{
      stream: stream,
      data: data,
      emitted_at: System.os_time(:millisecond)
    }
  end
end
