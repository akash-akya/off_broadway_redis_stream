defmodule OffBroadwayRedisStream.ProducerTest do
  use ExUnit.Case
  alias RedisHelper
  alias OffBroadwayRedisStream.RedisClient
  alias OffBroadwayRedisStream.RedisMock
  alias Broadway.Message
  import Mox

  defmodule Forwarder do
    use Broadway

    def handle_message(_, message, context) do
      content = %{data: message.data, metadata: message.metadata, pid: self()}
      send(context.test_pid, {:message_handled, content})

      case context.action do
        {:sleep, duration} -> Process.sleep(duration)
        {:fail, _count} -> raise "error"
        _ -> :ok
      end

      message
    end

    def handle_failed(messages, context) do
      Enum.map(messages, fn %{metadata: %{attempt: attempt}} = message ->
        case context.action do
          {:fail, max} when attempt < max ->
            Message.configure_ack(message, retry: true)

          _ ->
            message
        end
      end)
    end
  end

  setup :set_mox_from_context
  setup :verify_on_exit!

  @group "test-group"
  @stream "test"
  @redis_client RedisTestClient

  setup_all do
    {:ok, _} = Redix.start_link(host: host(), port: port(), name: :redix)
    :ok
  end

  setup do
    stub_with(@redis_client, OffBroadwayRedisStream.RedisMock)

    on_exit(fn ->
      RedisHelper.flushall(:redix)
    end)

    :ok
  end

  test "message" do
    RedisHelper.create_stream(:redix, @stream, @group)
    {:ok, pid} = start_broadway(@stream, group: @group)

    RedisHelper.xadd(:redix, @stream, "10000", foo: "bar")
    assert_receive {:message_handled, %{data: data, metadata: metadata}}
    assert data == ["10000-0", ["foo", "\"bar\""]]
    assert metadata == %{id: "10000-0", attempt: 1}

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
    assert 5 == RedisHelper.xlen(:redix, @stream)

    stop_broadway(pid)
  end

  test "producer acknowledging and deleting messages" do
    consumer = "baz"
    RedisHelper.create_stream(:redix, @stream, @group)

    {:ok, pid} =
      start_broadway(@stream,
        group: @group,
        consumer_name: consumer,
        delete_on_acknowledgment: true
      )

    push_messages(:redix, @stream, 1..5)
    :timer.sleep(10)

    assert [] == RedisHelper.xpending(:redix, @stream, @group, consumer)
    assert 0 == RedisHelper.xlen(:redix, @stream)

    stop_broadway(pid)
  end

  test "if producer claim pending messages after restart" do
    consumer = "test-cosnumer"
    Process.flag(:trap_exit, true)

    RedisHelper.create_stream(:redix, @stream, @group)
    push_messages(:redix, @stream, 1..5)

    {:ok, pid} =
      start_broadway(@stream, group: @group, consumer_name: consumer, action: {:sleep, 100_000})

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
        action: {:sleep, 100_000}
      )

    assert_receive {:message_handled, %{data: _}}
    refute_receive {:ack, _}

    pending = RedisHelper.xpending(:redix, @stream, @group, consumer1)
    assert length(pending) == 5

    Supervisor.stop(pid1, :kill, 100)
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

  test "ack failures" do
    consumer = "test-cosnumer-1"
    Process.flag(:trap_exit, true)

    RedisHelper.create_stream(:redix, @stream, @group)
    push_messages(:redix, @stream, 1..20)

    {:ok, pid} =
      start_broadway(@stream,
        group: @group,
        consumer_name: consumer,
        redis_command_retry_timeout: 1
      )

    @redis_client
    |> expect(:ack, 2, fn _, _ -> {:error, %RedisClient.ConnectionError{reason: :closed}} end)
    |> expect(:ack, 2, &RedisMock.ack/2)

    Process.sleep(50)

    stop_broadway(pid)
    assert [] == RedisHelper.xpending(:redix, @stream, @group, consumer)
  end

  test "fetch retry on redis connection failure" do
    consumer = "test-cosnumer-1"
    Process.flag(:trap_exit, true)

    RedisHelper.create_stream(:redix, @stream, @group)
    push_messages(:redix, @stream, 1..5)

    {:ok, pid} =
      start_broadway(@stream,
        group: @group,
        consumer_name: consumer,
        redis_command_retry_timeout: 1
      )

    {:ok, toggle} = Agent.start(fn -> :off end)

    @redis_client
    |> stub(:fetch, fn demand, last_id, config ->
      # if demand is 1 then its heartbeat request, skip that
      if demand > 1 && Agent.get(toggle, & &1) == :off do
        Agent.update(toggle, fn _ -> :on end)
        {:error, %RedisClient.ConnectionError{reason: :closed}}
      else
        OffBroadwayRedisStream.RedixClient.fetch(demand, last_id, config)
      end
    end)

    Process.sleep(50)
    stop_broadway(pid)

    assert_receive {:message_handled, %{data: _}}
    assert [] == RedisHelper.xpending(:redix, @stream, @group, consumer)
  end

  test "retries" do
    consumer = "test-cosnumer-1"
    Process.flag(:trap_exit, true)

    RedisHelper.create_stream(:redix, @stream, @group)
    push_messages(:redix, @stream, 1..5)

    attempts = 2

    {:ok, pid} =
      start_broadway(@stream,
        group: @group,
        consumer_name: consumer,
        action: {:fail, attempts}
      )

    for id <- 1..5, _ <- 1..attempts do
      id = "#{id}-0"
      assert_receive {:message_handled, %{data: [^id, _]}}
    end

    Process.sleep(50)
    stop_broadway(pid)

    assert [] == RedisHelper.xpending(:redix, @stream, @group, consumer)
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
    action = Keyword.get(opts, :action)
    redis_command_retry_timeout = Keyword.get(opts, :redis_command_retry_timeout, 5)
    delete_on_acknowledgment = Keyword.get(opts, :delete_on_acknowledgment, false)

    batchers =
      if batchers_concurrency do
        [default: [concurrency: batchers_concurrency, batch_size: 10, batch_timeout: 10]]
      else
        []
      end

    {:ok, pid} =
      Broadway.start_link(Forwarder,
        name: new_unique_name(),
        context: %{test_pid: self(), action: action},
        producer: [
          module:
            {OffBroadwayRedisStream.Producer,
             [
               redis_client_opts: redix_opts(),
               client: @redis_client,
               test_pid: self(),
               stream: stream,
               consumer_name: consumer_name,
               group: group,
               receive_interval: 0,
               heartbeat_interval: 100,
               allowed_missed_heartbeats: 2,
               redis_command_retry_timeout: redis_command_retry_timeout,
               delete_on_acknowledgment: delete_on_acknowledgment
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

  defp redix_opts, do: [host: host(), port: port()]

  defp host do
    System.get_env("REDIS_HOST") || "localhost"
  end

  defp port do
    if p = System.get_env("REDIS_PORT") do
      {port, ""} = Integer.parse(p)
      port
    else
      6379
    end
  end
end
