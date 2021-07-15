defmodule Airbyte.Source.GoogleAnalytics.Client do
  alias Airbyte.Source.GoogleAnalytics.ConnectionSpecification
  alias GoogleApi.Analytics.V3.{Api, Connection, Model}

  @unsupported_fields [
    "ga:customVarValueXX",
    "ga:customVarNameXX",
    "ga:calcMetric_<NAME>"
  ]

  def connection(%ConnectionSpecification{} = spec) do
    with {:ok, %Goth.Token{token: token}} <- spec |> get_token() do
      {:ok, token |> Connection.new()}
    end
  end

  def fields(conn) do
    conn
    |> get_all_fields()
    |> Stream.map(&to_fields_map/1)
    |> Enum.into(%{})
  end

  def profiles(conn) do
    with {:ok, summary} <- Api.Management.analytics_management_account_summaries_list(conn) do
      profiles =
        summary.items
        |> Enum.map(fn account -> account.webProperties end)
        |> List.flatten()
        |> Enum.map(fn property -> property.profiles end)
        |> List.flatten()

      {:ok, profiles}
    end
  end

  def get_error_message({:error, %Tesla.Env{body: body}}), do: parse_api_error(body)

  def get_error_message({:error, msg}), do: "Connection failed with: #{msg}"
  def get_error_message(_), do: "Connection failed with: unknown error"

  defp get_token(%ConnectionSpecification{service_account_key: path}) do
    with {:ok, json} <- path |> File.read!() |> Poison.decode(),
         account <- json["client_email"],
         :ok <- Goth.Config.add_config(json) do
      scope = "https://www.googleapis.com/auth/analytics.readonly"
      Goth.Token.for_scope({account, scope})
    end
  end

  defp to_fields_map(%Model.Column{id: id, attributes: %{"dataType" => dataType, "type" => type}}),
    do: {id, %{dataType: dataType, type: type}}

  defp to_fields_map(%Model.CustomDimension{id: id}),
    do: {id, %{dataType: "STRING", type: "DIMENSION"}}

  defp to_fields_map(%Model.CustomMetric{id: id, type: dataType}),
    do: {id, %{dataType: dataType, type: "METRIC"}}

  defp get_all_fields(conn) do
    [&get_standard_fields/1, &get_custom_dimensions/1, &get_custom_metrics/1]
    |> Task.async_stream(&(conn |> &1.()))
    |> Enum.reduce([], fn {:ok, list}, acc -> acc ++ list end)
  end

  defp get_standard_fields(conn) do
    with {:ok, columns} <- Api.Metadata.analytics_metadata_columns_list(conn, "ga") do
      columns.items
      |> Stream.reject(&reject_unsupported_fields/1)
      |> Stream.reject(&reject_deprecated_fields/1)
      |> Enum.to_list()
    end
  end

  defp reject_unsupported_fields(%Model.Column{id: id}), do: Enum.member?(@unsupported_fields, id)

  defp reject_deprecated_fields(%Model.Column{} = col),
    do: col.attributes["status"] == "DEPRECATED"

  defp get_custom_dimensions(conn),
    do: conn |> accounts() |> Enum.map(&get_account_custom_dimensions(conn, &1)) |> List.flatten()

  defp get_account_custom_dimensions(conn, %Model.AccountSummary{} = account),
    do: account.webProperties |> Enum.map(&get_web_property_custom_dimensions(conn, account, &1))

  defp get_web_property_custom_dimensions(
         conn,
         %Model.AccountSummary{} = account,
         %Model.WebPropertySummary{} = property
       ) do
    with {:ok, custom_dimensions} <-
           Api.Management.analytics_management_custom_dimensions_list(
             conn,
             account.id,
             property.id
           ) do
      custom_dimensions.items
    end
  end

  defp get_custom_metrics(conn) do
    conn
    |> accounts()
    |> Enum.map(&get_account_custom_metrics(conn, &1))
    |> List.flatten()
  end

  defp get_account_custom_metrics(conn, %Model.AccountSummary{} = account) do
    account.webProperties |> Enum.map(&get_web_property_custom_metrics(conn, account, &1))
  end

  defp get_web_property_custom_metrics(
         conn,
         %Model.AccountSummary{} = account,
         %Model.WebPropertySummary{} = property
       ) do
    with {:ok, custom_metrics} <-
           Api.Management.analytics_management_custom_metrics_list(
             conn,
             account.id,
             property.id
           ) do
      custom_metrics.items
    end
  end

  defp accounts(conn) do
    with {:ok, summary} <- Api.Management.analytics_management_account_summaries_list(conn) do
      summary.items
    end
  end

  defp parse_api_error(body) do
    case Jason.decode(body) do
      {:ok, body} ->
        "Connection failed with code #{body.error.code}: #{body.error.message}"

      _ ->
        "Connection failed with: #{body}"
    end
  end
end
