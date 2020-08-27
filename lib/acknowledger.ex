defmodule OffBroadwayRedisStream.Acknowledger do
  @moduledoc false
  @behaviour Broadway.Acknowledger

  @impl true
  def ack({producer_pid, _key}, successful, failed) do
    ids = Enum.map(successful ++ failed, fn %{acknowledger: {_, _, %{id: id}}} -> id end)
    send(producer_pid, {:ack, ids})
  end
end
