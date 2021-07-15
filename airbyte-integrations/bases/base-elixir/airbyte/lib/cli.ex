defmodule Airbyte.Cli do
  @moduledoc """
  Airbyte Cli module
  """

  @callback source() :: Airbyte.Source.t()

  defmacro __using__(_opts) do
    quote do
      @behaviour Airbyte.Cli

      alias Airbyte.Protocol.{
        AirbyteConnectionStatus,
        AirbyteLogMessage,
        AirbyteMessage,
        AirbyteStateMessage,
        ConfiguredAirbyteCatalog,
        ConnectorSpecification
      }

      def main([]), do: raise("No command specified")

      def main([command | args]) do
        with cmd <- command |> String.to_atom(),
             options <- parse(cmd, args),
             :ok <- run(cmd, options) do
        else
          e -> raise e
        end
      end

      defp run(:check, args) do
        with {:ok, status} <- source().check(args) do
          status |> AirbyteMessage.dispatch()
        else
          {:error, message} ->
            message
            |> AirbyteConnectionStatus.failed()
            |> AirbyteMessage.dispatch()
        end
      end

      defp run(:discover, args) do
        with {:ok, catalog} <- source().discover(args) do
          catalog |> AirbyteMessage.dispatch()
        else
          {:error, message} ->
            message |> AirbyteLogMessage.fatal() |> AirbyteMessage.dispatch()
        end
      end

      defp run(:read, args) do
        with {:ok, stream} <- source().read(args) do
          stream
          |> Stream.map(&AirbyteMessage.dispatch/1)
          |> Stream.run()
        else
          {:error, message} ->
            message |> AirbyteLogMessage.fatal() |> AirbyteMessage.dispatch()
        end
      end

      defp run(:spec, _) do
        with {:ok, spec} <- source().spec() do
          spec |> AirbyteMessage.dispatch()
        else
          {:error, message} ->
            message |> AirbyteLogMessage.fatal() |> AirbyteMessage.dispatch()
        end
      end

      defp run(command, _), do: raise("Unknown command: #{command}")

      defp parse(:check, args), do: parse_config(args)
      defp parse(:discover, args), do: parse_config(args)

      defp parse(:read, args) do
        [
          config: parse_config(args),
          catalog: parse_catalog(args),
          state: parse_state(args)
        ]
      end

      defp parse(_, _), do: nil

      defp parse_config(args) do
        [config: config] = parse_options(args, config: :string)
        spec = source().connection_specification_struct()

        config
        |> ConnectorSpecification.from_file(spec)
        |> Map.get(:connectionSpecification)
      end

      defp parse_catalog(args) do
        [catalog: catalog] = parse_options(args, catalog: :string)
        catalog |> ConfiguredAirbyteCatalog.from_file()
      end

      defp parse_state(args) do
        state = source().state_struct()

        case parse_options(args, state: :string) do
          [state: state] ->
            state_message = state |> AirbyteStateMessage.from_file()
            struct(state, state_message.data)
          _ -> struct(state, %{})
        end
      end

      defp parse_options(args, options) do
        {opts, _, _} = OptionParser.parse(args, strict: options)
        opts
      end
    end
  end
end
