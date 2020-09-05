defmodule OffBroadwayRedisStream.Heartbeat do
  @moduledoc false
  use GenServer
  require Logger
  alias OffBroadwayRedisStream.RedisClient

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

  @redis_command_retry_timeout 500
  @max_retries 2

  defp heartbeat(
         %{client: client, config: config, heartbeat_time: time} = state,
         retry_count \\ 0
       ) do
    # we refresh consumer idle time by attempting to read non-existent entry
    case client.fetch(1, @max_id, config) do
      {:ok, []} ->
        Process.send_after(self(), :heartbeat, time)

      {:error, %RedisClient.ConnectionError{} = error} when retry_count < @max_retries ->
        Logger.warn(
          "Failed to send heartbeat, retry_count: #{retry_count}, reason: #{inspect(error.reason)}"
        )

        Process.sleep(@redis_command_retry_timeout * (retry_count + 1))
        heartbeat(state, retry_count + 1)

      {:error, error} ->
        raise "Heartbeat failed, " <> inspect(error)
    end
  end
end
