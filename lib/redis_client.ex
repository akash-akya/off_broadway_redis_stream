defmodule OffBroadwayRedisStream.RedisClient do
  @moduledoc false
  alias Broadway.Message

  @type messages :: [Message.t()]

  @callback init(opts :: any) :: {:ok, any} | {:error, any}

  @callback receive_messages(demand :: pos_integer, opts :: any) :: messages
end
