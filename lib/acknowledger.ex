defmodule OffBroadwayRedisStream.Acknowledger do
  @moduledoc false
  @behaviour Broadway.Acknowledger

  @impl true
  def ack({producer_pid, _}, successful, failed) do
    {retryable, drop} =
      Enum.split_with(failed, fn
        %{acknowledger: {_, _ack_ref, ack_data}} -> ack_data.retry
      end)

    ack_ids =
      Enum.map(successful ++ drop, fn
        %{acknowledger: {_, _ack_ref, ack_data}} -> ack_data.id
      end)

    send(producer_pid, {:ack, ack_ids, retryable})
  end

  @impl true
  def configure(_ack_ref, ack_data, options) do
    case options do
      [retry: value] when is_boolean(value) ->
        {:ok, %{ack_data | retry: value}}

      _ ->
        {:error, "Invalid options, options must be keyword list with `:retry`"}
    end
  end
end
