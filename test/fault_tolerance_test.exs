defmodule OffBroadwayRedisStream.FaultToleranceTest do
  use ExUnit.Case

  alias OffBroadwayRedisStream.{Heartbeat, Producer, RedixClient, SlowRedixClient}

  import TestHelper

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
        context: %{agent: opts[:agent]},
        name: __MODULE__,
        producer: [
          module: {Producer, opts}
        ],
        processors: [
          default: [min_demand: 5, max_demand: 1000]
        ]
      )
    end

    def handle_message(_, message, context) do
      if agent_pid = context[:agent] do
        Agent.update(agent_pid, fn list -> [message | list] end)
      end

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

  test "slow heartbeat startup does not crash producer" do
    {:ok, agent} = Agent.start_link(fn -> [] end)
    {:ok, redix} = Redix.start_link(redix_opts())

    stream = "my-stream"
    group = "my-group"

    # delete the stream to ensure the group does not exist yet
    {:ok, _} = Redix.command(redix, ~w(DEL #{stream}))

    {:ok, _} = Redix.command(redix, ~w(XADD #{stream} * foo bar biz baz))

    opts = [
      redis_client_opts: Keyword.merge(redix_opts(),  sync_connect: false, exit_on_disconnection: false),
      client: SlowRedixClient,
      stream: stream,
      group: group,
      make_stream: true,
      group_start_id: "0",
      consumer_name: "consumer",
      agent: agent,
      heartbeat_sleep: 500
    ]

    assert {:ok, pid } = DummyProducer.start_link(opts)

    Process.sleep(2_000)

    messages = Agent.get(agent, fn  list -> list end)
    assert length(messages) == 1

    {:ok, _} = Redix.command(redix, ~w(DEL #{stream}))

    stop_process(pid)
    stop_process(redix)
    stop_process(agent)
  end

end
