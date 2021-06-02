# OffBroadwayRedisStream [![Hex.pm](https://img.shields.io/hexpm/v/off_broadway_redis_stream.svg)](https://hex.pm/packages/off_broadway_redis_stream)

A Redis Stream consumer for [Broadway](https://github.com/dashbitco/broadway).

Broadway producer acts as a consumer in the specified Redis consumer group. Introduction to Redis Stream can be found at: https://redis.io/topics/streams-intro.

Support failover by automatically claiming pending messages of a dead node. A node is considered dead when it fails send heartbeats.

```elixir
defmodule MyBroadway do
  use Broadway

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module:
          {OffBroadwayRedisStream.Producer,
           [
             redis_client_opts: [host: "localhost"],
             stream: "orders",
             group: "processor-group",
             consumer_name: hostname()
           ]}
      ],
      processors: [
        default: [min_demand: 5, max_demand: 1000]
      ]
    )
  end

  def handle_message(_, message, _) do
    [_id, key_value_list] = message.data
    IO.inspect(key_value_list, label: "Got message")
    message
  end

  @max_attempts 5

  def handle_failed(messages, _) do
    for message <- messages do
      if message.metadata.attempt < @max_attempts do
        Broadway.Message.configure_ack(message, retry: true)
      else
        [id, _] = message.data
        IO.inspect(id, label: "Dropping")
      end
    end
  end

  defp hostname do
    {:ok, host} = :inet.gethostname()
    to_string(host)
  end
end
```

Currently, it only supports Redis 6.0.2 and above.

Please check module documentation for more information

## Installation

```elixir
def deps do
  [
    {:off_broadway_redis_stream, "~> x.x.x"}
  ]
end
```

### License

Copyright (c) 2021 Akash Hiremath.

OffBroadwayRedisStream source code is released under Apache License 2.0. Check [LICENSE](LICENSE.md) for more information.
