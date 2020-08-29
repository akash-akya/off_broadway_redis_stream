defmodule OffBroadwayRedisStream.ProducerTest do
  use ExUnit.Case
  alias RedisHelper
  alias OffBroadwayRedisStream.RedisMock

  defmodule Forwarder do
    use Broadway

    def handle_message(_, message, %{test_pid: test_pid, test_sleep_duration: duration}) do
      content = %{data: message.data, metadata: message.metadata, pid: self()}
      send(test_pid, {:message_handled, content})
      Process.sleep(duration)
      message
    end

    def handle_batch(_, messages, _batch_info, %{
          test_pid: test_pid,
          test_sleep_duration: duration
        }) do
      content = %{id: List.last(messages).metadata.id, pid: self()}
      Process.sleep(duration)
      send(test_pid, {:batch_handled, content})
      messages
    end
  end

  setup do
    on_exit(fn ->
      RedisHelper.flushall(:redix)
    end)
  end

  @group "test-group"
  @stream "test"

  test "message" do
    RedisHelper.create_stream(:redix, @stream, @group)
    {:ok, pid} = start_broadway(@stream, group: @group)

    RedisHelper.xadd(:redix, @stream, "10000", foo: "bar")
    assert_receive {:message_handled, %{data: data, metadata: metadata}}
    assert data == ["10000-0", ["foo", "\"bar\""]]
    assert metadata == %{id: "10000-0"}

    stop_broadway(pid)
  end

  test "producer receiving multiple messages" do
    RedisHelper.create_stream(:redix, @stream, @group)
    {:ok, pid} = start_broadway(@stream, group: @group)

    push_messages(:redix, @stream, 1..5)

    for id <- 1..5 do
      id = "#{id}-0"
      assert_receive {:message_handled, %{data: [^id, ["foo", _]]}}
    end

    stop_broadway(pid)
  end

  test "producer acknowledging messages" do
    consumer = "baz"
    RedisHelper.create_stream(:redix, @stream, @group)
    {:ok, pid} = start_broadway(@stream, group: @group, consumer_name: consumer)

    push_messages(:redix, @stream, 1..5)
    :timer.sleep(10)

    assert [] == RedisHelper.xpending(:redix, @stream, @group, consumer)

    stop_broadway(pid)
  end

  test "if producer claim pending messages after restart" do
    consumer = "test-cosnumer"
    Process.flag(:trap_exit, true)

    RedisHelper.create_stream(:redix, @stream, @group)
    push_messages(:redix, @stream, 1..5)

    {:ok, pid} =
      start_broadway(@stream, group: @group, consumer_name: consumer, test_sleep_duration: 100_000)

    assert_receive {:message_handled, %{data: _}}
    refute_receive {:ack, _}

    Supervisor.stop(pid, :kill, 10)

    pending = RedisHelper.xpending(:redix, @stream, @group, consumer)
    assert length(pending) == 5

    flush_message_handled()
    {:ok, pid} = start_broadway(@stream, group: @group, consumer_name: consumer)
    producer_pid = Process.whereis(get_producer(pid))

    assert_receive {:message_handled, %{data: ["5-0", _]}}, 500
    assert_receive {:ack, %{ids: _, pid: ^producer_pid}}
    assert [] == RedisHelper.xpending(:redix, @stream, @group, consumer)

    stop_broadway(pid)
  end

  test "if producer claim pending messages from dead producer" do
    consumer1 = "test-cosnumer-1"
    Process.flag(:trap_exit, true)

    RedisHelper.create_stream(:redix, @stream, @group)
    push_messages(:redix, @stream, 1..5)

    {:ok, pid1} =
      start_broadway(@stream,
        group: @group,
        consumer_name: consumer1,
        test_sleep_duration: 100_000
      )

    assert_receive {:message_handled, %{data: _}}
    refute_receive {:ack, _}

    pending = RedisHelper.xpending(:redix, @stream, @group, consumer1)
    assert length(pending) == 5

    Supervisor.stop(pid1, :kill, 10)
    flush_all_messages()
    # wait for consumer1 to cross allowed_missed_heartbeats
    Process.sleep(500)

    consumer2 = "test-cosnumer-2"
    {:ok, pid2} = start_broadway(@stream, group: @group, consumer_name: consumer2)
    producer_pid = Process.whereis(get_producer(pid2))

    assert_receive({:message_handled, %{data: _}}, 5000)
    assert_receive {:ack, %{ids: _, pid: ^producer_pid}}
    assert [] == RedisHelper.xpending(:redix, @stream, @group, consumer1)

    stop_broadway(pid2)
  end

  defp push_messages(pid, stream, ids) do
    for id <- ids do
      RedisHelper.xadd(pid, stream, to_string(id), foo: "bar-#{id}")
    end
  end

  defp start_broadway(stream, opts) do
    producers_concurrency = Keyword.get(opts, :producers_concurrency, 1)
    processors_concurrency = Keyword.get(opts, :processors_concurrency, 1)
    batchers_concurrency = Keyword.get(opts, :batchers_concurrency)
    group = Keyword.get(opts, :group, "test-group")
    consumer_name = Keyword.get(opts, :consumer_name, "test")
    test_sleep_duration = Keyword.get(opts, :test_sleep_duration, 0)

    batchers =
      if batchers_concurrency do
        [default: [concurrency: batchers_concurrency, batch_size: 10, batch_timeout: 10]]
      else
        []
      end

    {:ok, pid} =
      Broadway.start_link(Forwarder,
        name: new_unique_name(),
        context: %{test_pid: self(), test_sleep_duration: test_sleep_duration},
        producer: [
          module:
            {OffBroadwayRedisStream.Producer,
             [
               redis_instance: :redix,
               client: RedisMock,
               test_pid: self(),
               stream: stream,
               consumer_name: consumer_name,
               group: group,
               receive_interval: 0,
               heartbeat_time: 100,
               allowed_missed_heartbeats: 2
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

  defp flush_message_handled() do
    receive do
      {:message_handled, _} -> flush_message_handled()
    after
      0 -> :ok
    end
  end

  defp flush_all_messages() do
    receive do
      _ -> flush_message_handled()
    after
      0 -> :ok
    end
  end

  defp stop_broadway(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end
end
