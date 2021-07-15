defmodule Airbyte.Source.GoogleAnalytics.Schema do
  alias GoogleApi.Analytics.V3.Model

  require Logger

  def parse(%Model.GaDataColumnHeaders{dataType: "CURRENCY"}), do: &String.to_float/1
  def parse(%Model.GaDataColumnHeaders{dataType: "FLOAT"}), do: &String.to_float/1
  def parse(%Model.GaDataColumnHeaders{dataType: "INTEGER"}), do: &String.to_integer/1
  def parse(%Model.GaDataColumnHeaders{dataType: "PERCENT"}), do: &String.to_float/1
  def parse(%Model.GaDataColumnHeaders{dataType: "STRING"}), do: &Function.identity/1
  def parse(%Model.GaDataColumnHeaders{dataType: "TIME"}), do: &String.to_float/1

  def parse(%Model.GaDataColumnHeaders{dataType: type}) do
    Logger.warn("Unrecognized type: #{type}")
    &Function.identity/1
  end

  def to_type("CURRENCY"), do: "number"
  def to_type("FLOAT"), do: "number"
  def to_type("INTEGER"), do: "number"
  def to_type("PERCENT"), do: "number"
  def to_type("STRING"), do: "string"
  def to_type("TIME"), do: "number"
  def to_type(_), do: "string"

  def to_field_name(val) do
    val
    |> String.replace(~r/^ga:/, "")
    |> Macro.underscore()
  end
end
