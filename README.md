# OffBroadwayRedisStream

[![CI](https://github.com/akash-akya/off_broadway_redis_stream/actions/workflows/elixir.yml/badge.svg)](https://github.com/akash-akya/off_broadway_redis_stream/actions/workflows/elixir.yml)
[![Hex.pm](https://img.shields.io/hexpm/v/off_broadway_redis_stream.svg)](https://hex.pm/packages/off_broadway_redis_stream)
[![docs](https://img.shields.io/badge/docs-hexpm-blue.svg)](https://hexdocs.pm/off_broadway_redis_stream/)

A Redis Stream consumer for [Broadway](https://github.com/dashbitco/broadway).
A Redis Stream consumer implementation for [Broadway](https://github.com/dashbitco/broadway), enabling concurrent processing of Redis Stream messages with built-in fault tolerance and automatic failover.


## Features

- Seamless integration with Broadway for concurrent message processing
- Automatic failover and message recovery when nodes fail
- Configurable consumer groups for distributed processing
- Message retry handling with configurable attempts

It supports failover by automatically claiming pending messages when a node dies. A node is considered dead when it fails to send heartbeats.

## Installation

Add `off_broadway_redis_stream` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:off_broadway_redis_stream, "~> x.x.x"}
  ]
end
```

## Usage

### Basic Setup

Here's a complete example demonstrating how to set up a Broadway pipeline with Redis Stream:

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

Check [documentation](https://hex.pm/packages/off_broadway_redis_stream) for more details.

### License

Copyright (c) 2021 Akash Hiremath.

OffBroadwayRedisStream source code is released under Apache License 2.0. Check [LICENSE](LICENSE.md) for more information.
