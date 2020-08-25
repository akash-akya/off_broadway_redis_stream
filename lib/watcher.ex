defmodule OffBroadwayRedisStream.Watcher do
  @moduledoc false
  use GenServer
  require Logger

  def start_link(client, heartbeat_time) do
    GenServer.start_link(__MODULE__, {client, heartbeat_time})
  end

  @default_heartbeat_time 500

  @impl true
  def init({client, heartbeat_time}) do
    state = %{client: client, heartbeat_time: heartbeat_time}
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

  defp heartbeat(%{client: {redis_client, redis_opts}, heartbeat_time: time}) do
    :ok = redis_client.heartbeat(redis_opts)
    Process.send_after(self(), :heartbeat, time)
  end
end
