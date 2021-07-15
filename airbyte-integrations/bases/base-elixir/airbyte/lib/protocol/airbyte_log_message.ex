defmodule Airbyte.Protocol.AirbyteLogMessage do
  @moduledoc """
  Specification of an AirbyteLogMessage
  https://github.com/airbytehq/airbyte/blob/master/airbyte-protocol/models/src/main/resources/airbyte_protocol/airbyte_protocol.yaml
  """
  use TypedStruct

  @derive Jason.Encoder

  typedstruct do
    @typedoc "Specification of an AirbyteLogMessage"

    field(:level, String.t(), enforce: true)
    field(:message, String.t(), enforce: true)
  end

  def fatal(message), do: %__MODULE__{level: "FATAL", message: message}
  def error(message), do: %__MODULE__{level: "ERROR", message: message}
  def warn(message), do: %__MODULE__{level: "WARN", message: message}
  def info(message), do: %__MODULE__{level: "INFO", message: message}
  def debug(message), do: %__MODULE__{level: "DEBUG", message: message}
  def trace(message), do: %__MODULE__{level: "TRACE", message: message}
end
