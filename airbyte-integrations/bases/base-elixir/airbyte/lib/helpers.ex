defmodule Airbyte.Helpers do
  def json_to_struct(path, to_struct) do
    with {:ok, contents} <- path |> File.read(),
         {:ok, decoded} <- contents |> Jason.decode(keys: :atoms) do
      struct(to_struct, decoded)
    else
      {:error, :enoent} -> raise "Could not find file: #{path}"
    end
  end
end
