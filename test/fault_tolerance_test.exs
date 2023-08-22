defmodule OffBroadwayRedisStream.FaultToleranceTest do
  use ExUnit.Case

  alias OffBroadwayRedisStream.{Heartbeat, Producer, RedixClient}

  @redis_opts [
    host: "bad-host-name",
    port: 9123,
    sync_connect: false,
    exit_on_disconnection: false
  ]

  @opts [
    redis_client_opts: @redis_opts,
    stream: "test",
    make_stream: true,
    group: "test-group",
    consumer_name: "test-consumer"
  ]

  defmodule DummyProducer do
    use Broadway

    def start_link(opts) do
      Broadway.start_link(__MODULE__,
        name: __MODULE__,
        producer: [
          module: {Producer, opts}
        ],
        processors: [
          default: [min_demand: 5, max_demand: 1000]
        ]
      )
    end

    def handle_message(_, message, _) do
      message
    end
  end

  test "producer starts with unreachable redis" do
    assert {:ok, pid} = DummyProducer.start_link(@opts)

    stop_process(pid)
  end

  test "producer does not kill supervisor" do
    children = [
      {DummyProducer, @opts}
    ]

    opts = [strategy: :one_for_one]
    {:ok, pid} = Supervisor.start_link(children, opts)

    Process.sleep(1000)
    assert Process.alive?(pid)

    stop_process(pid)
  end

  test "heartbeat does not kill supervisor with unreachable redis" do
    assert {:ok, redis_config} = RedixClient.init(@opts)

    children = [
      {Heartbeat, client: RedixClient, config: redis_config, heartbeat_interval: 5_000}
    ]

    opts = [strategy: :one_for_one]
    {:ok, pid} = Supervisor.start_link(children, opts)

    Process.sleep(1000)
    assert Process.alive?(pid)

    stop_process(pid)
  end

  def stop_process(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end
end
