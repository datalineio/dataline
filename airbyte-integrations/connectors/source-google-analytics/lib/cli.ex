defmodule Airbyte.Source.GoogleAnalytics.Cli do
  use Airbyte.Cli

  @impl Airbyte.Cli
  def source(), do: Airbyte.Source.GoogleAnalytics.Source
end
