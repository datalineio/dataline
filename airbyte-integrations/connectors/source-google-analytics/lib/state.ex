defmodule Airbyte.Source.GoogleAnalytics.State do
  use TypedStruct

  alias Airbyte.Protocol.{AirbyteStateMessage}
  alias Airbyte.Source.GoogleAnalytics.ConnectionSpecification

  @derive Jason.Encoder

  typedstruct do
    field(:latest_sync_date, String.t(), enforce: true)
    field(:start_date, String.t())
    field(:end_date, String.t())
  end

  def from(
        %ConnectionSpecification{start_date: start_date},
        %__MODULE__{latest_sync_date: nil} = state
      ) do
    %__MODULE__{state | start_date: start_date, end_date: yesterday()}
  end

  def from(%ConnectionSpecification{}, %__MODULE__{latest_sync_date: sync_date} = state) do
    %__MODULE__{state | start_date: sync_date |> day_after(), end_date: yesterday()}
  end

  def to_airbyte_state_message(%__MODULE__{} = state) do
    %__MODULE__{state | latest_sync_date: state.end_date}
    |> AirbyteStateMessage.from_data()
  end

  defp yesterday() do
    Date.utc_today() |> Date.add(-1) |> Date.to_iso8601()
  end

  defp day_after(date) do
    date |> Date.from_iso8601!() |> Date.add(1) |> Date.to_iso8601()
  end
end
