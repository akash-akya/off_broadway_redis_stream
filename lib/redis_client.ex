defmodule OffBroadwayRedisStream.RedisClient do
  @moduledoc false

  defmodule ConnectionError do
    @type t :: %__MODULE__{reason: atom}

    defexception [:reason, :message]
  end

  @type id :: String.t()
  @type message :: [any()]

  @callback init(config :: keyword) :: {:ok, any} | {:error, any}

  @callback fetch(
              demand :: pos_integer,
              last_id :: id,
              config :: keyword
            ) :: {:ok, messages :: [message()]} | {:error, ConnectionError.t()} | {:error, any}

  @callback consumers_info(config :: keyword) ::
              {:ok, any} | {:error, ConnectionError.t()} | {:error, any}

  @callback create_group(offset :: String.t(), config :: keyword) ::
              :ok | {:error, ConnectionError.t()} | {:error, any}

  @callback pending(
              consumer :: String.t(),
              count :: pos_integer(),
              config :: keyword
            ) :: {:ok, any} | {:error, ConnectionError.t()} | {:error, any}

  @callback claim(
              idle :: pos_integer,
              ids :: [id],
              config :: keyword
            ) :: {:ok, messages :: [message()]} | {:error, ConnectionError.t()} | {:error, any}

  @callback ack(ids :: [id], config :: keyword) ::
              :ok | {:error, ConnectionError.t()} | {:error, any}
end
