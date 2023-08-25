defmodule OffBroadwayRedisStream.Heartbeat do
  @moduledoc false
  use GenServer
  require Logger
  alias OffBroadwayRedisStream.RedisClient

  @max_id round(:math.pow(2, 64)) - 1

  def start_link(opts) do
    start_link(opts[:client], opts[:config], opts[:heartbeat_interval])
  end

  def start_link(client, config, heartbeat_interval) do
    GenServer.start_link(__MODULE__, {client, config, heartbeat_interval})
  end

  def stop(pid) do
    GenServer.stop(pid, :shutdown)
  end

  @impl true
  def init({client, config, heartbeat_interval}) do
    state = %{client: client, config: config, heartbeat_interval: heartbeat_interval}
    {:ok, state, {:continue, nil}}
  end

  @impl true
  def handle_continue(nil, state) do
    create_group(state)
    heartbeat(state)
    {:noreply, state}
  end

  @impl true
  def handle_info(:heartbeat, state) do
    heartbeat(state)
    {:noreply, state}
  end

  @redis_command_retry_timeout 300
  @max_retries 2

  defp create_group(%{client: client, config: config} = state, retry_count \\ 0) do
    # we need to ensure the group is created before a heartbeat will succeed
    case client.create_group(config[:group_start_id], config) do
      :ok ->
        :ok

      {:error, %RedisClient.ConnectionError{} = error} when retry_count < @max_retries ->
        Logger.warn(
          "Failed to create group, retry_count: #{retry_count}, reason: #{inspect(error.reason)}"
        )

        Process.sleep(@redis_command_retry_timeout * (retry_count + 1))
        create_group(state, retry_count + 1)

      {:error, error} ->
        raise "Create group failed, " <> inspect(error)
    end
  end

  defp heartbeat(
         %{client: client, config: config, heartbeat_interval: interval} = state,
         retry_count \\ 0
       ) do
    # we refresh consumer idle time by attempting to read non-existent entry
    case client.fetch(1, @max_id, config) do
      {:ok, []} ->
        Process.send_after(self(), :heartbeat, interval)

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
