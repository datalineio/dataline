defmodule Airbyte.Protocol.ConfiguredAirbyteCatalog do
  @moduledoc """
  Configured Airbyte Catalog
  https://github.com/airbytehq/airbyte/blob/master/airbyte-protocol/models/src/main/resources/airbyte_protocol/airbyte_protocol.yaml
  """
  use TypedStruct

  alias Airbyte.Protocol.{ConfiguredAirbyteCatalog, ConfiguredAirbyteStream}
  alias Airbyte.Helpers

  @derive Jason.Encoder

  typedstruct do
    @typedoc "Configured Airbyte Catalog"
    field(:streams, list(ConfiguredAirbyteStream.t()), enforce: true)
  end

  def from_file(path) do
    %__MODULE__{streams: streams} = Helpers.json_to_struct(path, __MODULE__)
    configured = streams |> Enum.map(&struct(ConfiguredAirbyteStream, &1))

    %ConfiguredAirbyteCatalog{streams: configured}
  end
end
