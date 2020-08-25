defmodule OffBroadwayRedisStream.RedisClient do
  @moduledoc false
  alias Broadway.Message

  @type messages :: [Message.t()]

  @type opts :: any

  @callback init(opts) :: {:ok, any} | {:error, any}

  @callback receive_messages(demand :: pos_integer, opts) :: {messages, opts}

  @callback heartbeat(opts) :: {:ok, any} | {:error, any}

  @callback consumers_info(opts) :: {:ok, any} | {:error, any}

  @callback pending(opts, consumer :: String.t(), count :: pos_integer) ::
              {:ok, any} | {:error, any}

  @callback claim(opts, idle :: pos_integer, ids :: [String.t()]) :: {:ok, any} | {:error, any}
end
