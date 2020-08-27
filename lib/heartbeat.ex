defmodule OffBroadwayRedisStream.Heartbeat do
  @moduledoc false
  use GenServer
  require Logger

  @max_id round(:math.pow(2, 64)) - 1

  def start_link(client, config, heartbeat_time) do
    GenServer.start_link(__MODULE__, {client, config, heartbeat_time})
  end

  @impl true
  def init({client, config, heartbeat_time}) do
    state = %{client: client, config: config, heartbeat_time: heartbeat_time}
    {:ok, state, {:continue, nil}}
  end

  @impl true
  def handle_continue(nil, state) do
    heartbeat(state)
    {:noreply, state}
  end

  @impl true
  def handle_info(:heartbeat, state) do
    heartbeat(state)
    {:noreply, state}
  end

  defp heartbeat(%{client: client, config: config, heartbeat_time: time}) do
    # we refresh consumer idle time by attempting to read non-existent entry
    case client.fetch(1, @max_id, config) do
      {:ok, []} -> :ok
      {:error, error} -> raise "Heartbeat failed, " <> inspect(error)
    end

    Process.send_after(self(), :heartbeat, time)
  end
end
