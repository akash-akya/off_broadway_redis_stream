defmodule OffBroadwayRedisStream.ProducerTest do
  use ExUnit.Case
  alias RedisHelper

  defmodule Forwarder do
    use Broadway

    def handle_message(_, message, %{test_pid: test_pid}) do
      content = %{data: message.data, metadata: message.metadata, pid: self()}
      send(test_pid, {:message_handled, content})
      message
    end

    def handle_batch(_, messages, _batch_info, %{test_pid: test_pid}) do
      content = %{id: List.last(messages).metadata.id, pid: self()}
      send(test_pid, {:batch_handled, content})
      messages
    end
  end

  setup do
    on_exit(fn ->
      RedisHelper.flushall(:redix)
    end)
  end

  test "message" do
    stream = "test"
    group = "test-group"
    RedisHelper.create_stream(:redix, stream, group)
    {:ok, pid} = start_broadway(stream, group: group)

    RedisHelper.xadd(:redix, stream, "10000", foo: "bar")
    assert_receive {:message_handled, %{data: data, metadata: metadata}}
    assert data == ["10000-0", ["foo", "\"bar\""]]
    assert metadata == %{id: "10000-0"}

    stop_broadway(pid)
  end

  test "producer receiving multiple messages" do
    stream = "test"
    group = "test-group"
    RedisHelper.create_stream(:redix, stream, group)
    {:ok, pid} = start_broadway(stream, group: group)

    push_messages(:redix, stream, 1..5)

    for id <- 1..5 do
      id = "#{id}-0"
      assert_receive {:message_handled, %{data: [^id, ["foo", _]]}}
    end

    stop_broadway(pid)
  end

  test "producer acknowledging messages" do
    stream = "test"
    group = "test-group"
    consumer = "baz"
    RedisHelper.create_stream(:redix, stream, group)
    {:ok, pid} = start_broadway(stream, group: group, consumer: consumer)

    push_messages(:redix, stream, 1..5)
    :timer.sleep(10)

    assert [] == RedisHelper.xpending(:redix, stream, group, consumer)

    stop_broadway(pid)
  end

  defp push_messages(pid, stream, ids) do
    for id <- ids do
      RedisHelper.xadd(pid, stream, to_string(id), foo: "bar-#{id}")
    end
  end

  defp start_broadway(stream, opts \\ []) do
    producers_concurrency = Keyword.get(opts, :producers_concurrency, 1)
    processors_concurrency = Keyword.get(opts, :processors_concurrency, 1)
    batchers_concurrency = Keyword.get(opts, :batchers_concurrency)
    ack_raises_on_id = Keyword.get(opts, :ack_raises_on_id, nil)
    group = Keyword.get(opts, :group, "test-group")
    consumer_name = Keyword.get(opts, :consumer_name, "test")

    batchers =
      if batchers_concurrency do
        [default: [concurrency: batchers_concurrency, batch_size: 10, batch_timeout: 10]]
      else
        []
      end

    {:ok, pid} =
      Broadway.start_link(Forwarder,
        name: new_unique_name(),
        context: %{test_pid: self()},
        producer: [
          module:
            {OffBroadwayRedisStream.Producer,
             [
               redis_instance: :redix,
               hosts: [],
               test_pid: self(),
               stream: stream,
               consumer_name: consumer_name,
               group: group,
               receive_interval: 0,
               ack_raises_on_id: ack_raises_on_id
             ]},
          concurrency: producers_concurrency
        ],
        processors: [
          default: [concurrency: processors_concurrency]
        ],
        batchers: batchers
      )

    {:ok, pid}
  end

  defp new_unique_name() do
    :"Broadway#{System.unique_integer([:positive, :monotonic])}"
  end

  defp get_producer(broadway, index \\ 0) do
    {_, name} = Process.info(broadway, :registered_name)
    :"#{name}.Broadway.Producer_#{index}"
  end

  defp stop_broadway(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end
end
