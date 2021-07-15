defmodule Airbyte.Protocol.AirbyteStream do
  @moduledoc """
  Specification of an AirbyteStream
  https://github.com/airbytehq/airbyte/blob/master/airbyte-protocol/models/src/main/resources/airbyte_protocol/airbyte_protocol.yaml
  """
  use TypedStruct

  @derive Jason.Encoder

  typedstruct do
    @typedoc "Specification of an AirbyteStream"
    field(:name, String.t(), enforce: true)
    field(:json_schema, any(), enforce: true)
    field(:supported_sync_modes, list(String.t()), enforce: true)
    field(:source_defined_cursor, boolean())
    field(:default_cursor_field, list(String.t()))
  end
end
