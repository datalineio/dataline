defmodule Airbyte.Protocol.ConfiguredAirbyteStream do
  @moduledoc """
  Specification of an ConfiguredAirbyteStream
  https://github.com/airbytehq/airbyte/blob/master/airbyte-protocol/models/src/main/resources/airbyte_protocol/airbyte_protocol.yaml
  """
  use TypedStruct

  alias Airbyte.Protocol.AirbyteStream

  @derive Jason.Encoder

  typedstruct do
    @typedoc "Specification of an ConfiguredAirbyteStream"
    field(:stream, AirbyteStream.t(), enforce: true)
    field(:sync_mode, String.t(), default: "full_refresh")
    field(:cursor_field, list(String.t()))
  end
end
