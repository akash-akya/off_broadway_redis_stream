defmodule OffBroadwayRedisStream.Heartbeat do
  @moduledoc false
  use GenServer
  require Logger

  def start_link(client, opts, heartbeat_time) do
    GenServer.start_link(__MODULE__, {client, opts, heartbeat_time})
  end

  @impl true
  def init({client, opts, heartbeat_time}) do
    state = %{client: client, opts: opts, heartbeat_time: heartbeat_time}
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

  defp heartbeat(%{client: client, opts: opts, heartbeat_time: time}) do
    :ok = client.heartbeat(opts)
    Process.send_after(self(), :heartbeat, time)
  end
end
