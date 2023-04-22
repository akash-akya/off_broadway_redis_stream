defmodule OffBroadwayRedisStream.RedisMock do
  @moduledoc false

  @behaviour OffBroadwayRedisStream.RedisClient
  alias OffBroadwayRedisStream.RedixClient, as: Super

  @impl true
  def init(opts), do: Super.init(opts)

  @impl true
  def fetch(demand, last_id, config) do
    {:ok, messages} = Super.fetch(demand, last_id, config)
    send(config[:test_pid], {:messages_fetched, messages})
    {:ok, messages}
  end

  @impl true
  def ack(ids, config) do
    info = %{ids: ids, pid: self()}
    Super.ack(ids, config)
    send(config[:test_pid], {:ack, info})
    :ok
  end

  @impl true
  def delete_message(ids, config) do
    info = %{ids: ids, pid: self()}
    Super.delete_message(ids, config)
    send(config[:test_pid], {:delete_message, info})
    :ok
  end

  @impl true
  def consumers_info(config) do
    {:ok, info} = Super.consumers_info(config)
    send(config[:test_pid], {:consumer_info, info})
    {:ok, info}
  end

  @impl true
  def create_group(offset, config) do
    Super.create_group(offset, config)
  end

  @impl true
  def pending(consumer, count, config) do
    {:ok, ids} = Super.pending(consumer, count, config)
    send(config[:test_pid], {:pending, ids})
    {:ok, ids}
  end

  @impl true
  def claim(idle, ids, config) do
    {:ok, messages} = Super.claim(idle, ids, config)
    send(config[:test_pid], {:claim, ids, messages})
    {:ok, messages}
  end

  @impl true
  def delete_consumers(consumers, config) do
    Super.delete_consumers(consumers, config)
  end
end
