defmodule Airbyte.Protocol.ConnectorSpecification do
  @moduledoc """
  Specification of a connector (source/destination)
  https://github.com/airbytehq/airbyte/blob/master/airbyte-protocol/models/src/main/resources/airbyte_protocol/airbyte_protocol.yaml
  """
  use TypedStruct

  alias Airbyte.Helpers

  @derive Jason.Encoder

  typedstruct do
    @typedoc "Specification of a connector (source/destination)"

    field(:documentationUrl, String.t())
    field(:changelogUrl, String.t())
    field(:connectionSpecification, map(), enforce: true)
    field(:supportsIncremental, boolean())
  end

  def from_file(path, conn_spec_module \\ %{}) do
    spec = Helpers.json_to_struct(path, __MODULE__)
    conn_spec = struct(conn_spec_module, spec.connectionSpecification)
    %__MODULE__{spec | connectionSpecification: conn_spec}
  end
end
