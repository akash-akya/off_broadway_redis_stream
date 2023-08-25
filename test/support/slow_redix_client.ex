defmodule OffBroadwayRedisStream.SlowRedixClient do
  @moduledoc false

  @behaviour OffBroadwayRedisStream.RedisClient

  alias OffBroadwayRedisStream.RedixClient

  @impl true
  defdelegate init(config), to: RedixClient

  @impl true
  defdelegate fetch(demand, last_id, config), to: RedixClient

  @impl true
  defdelegate consumers_info(config), to: RedixClient

  @impl true
  def create_group(id, config) do
    Process.sleep(config[:heartbeat_sleep])
    RedixClient.create_group(id, config)
  end

  @impl true
  defdelegate pending(consumer, count, config), to: RedixClient

  @impl true
  defdelegate claim(idle, ids, config), to: RedixClient

  @impl true
  defdelegate ack(ids, config), to: RedixClient

  @impl true
  defdelegate delete_message(ids, config), to: RedixClient

  @impl true
  defdelegate delete_consumers(consumers, config), to: RedixClient
end
