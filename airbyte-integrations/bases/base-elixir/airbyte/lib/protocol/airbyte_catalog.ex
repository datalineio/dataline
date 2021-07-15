defmodule Airbyte.Protocol.AirbyteCatalog do
  @moduledoc """
  Airbyte stream schema catalog
  https://github.com/airbytehq/airbyte/blob/master/airbyte-protocol/models/src/main/resources/airbyte_protocol/airbyte_protocol.yaml
  """
  use TypedStruct

  alias Airbyte.Protocol.AirbyteStream

  @derive Jason.Encoder

  typedstruct do
    @typedoc "Airbyte stream schema catalog"
    field(:streams, list(AirbyteStream.t()), enforce: true)
  end

  def create(streams) do
    %__MODULE__{streams: streams}
  end
end
