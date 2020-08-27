defmodule OffBroadwayRedisStream.ProducerTest do
  use ExUnit.Case

  defmodule MessageServer do
    def start_link() do
      Agent.start_link(fn -> [] end)
    end

    def push_messages(server, messages) do
      Agent.update(server, fn queue -> queue ++ messages end)
    end

    def take_messages(server, amount) do
      Agent.get_and_update(server, fn queue ->
        {messages, rest} = Enum.split(queue, amount)
        {messages, Map.put(queue, key, rest)}
      end)
    end
  end

  defmodule FakeRedisClient do
    @behaviour OffBroadwayRedisStream.RedisClient

    @impl true
    def init(opts), do: {:ok, Map.new(opts)}

    @impl true
    def fetch(demand, last_id, config) do
      messages = MessageServer.take_messages(config[:message_server], demand)
      send(config[:test_pid], {:messages_fetched, length(messages)})
      {:ok, messages}
    end

    @impl true
    def ack(ids, config) do
      info = %{ids: ids, pid: self()}
      ack_raises_on_id = config[:ack_raises_on_id]

      if ack_raises_on_id in ids do
        raise "Ack failed on id #{ack_raises_on_id}"
      end

      send(config[:test_pid], {:ack, info})
    end

    @impl true
    def consumers_info(config) do
      ack_raises_on_id = config[:ack_raises_on_id]

      if ack_raises_on_id in ids do
        raise "Ack failed on id #{ack_raises_on_id}"
      end

      send(config[:test_pid], {:ack, info})
    end

    defp create_id(i), String.pad_leading(to_string(i), 10, "0")
  end
end
