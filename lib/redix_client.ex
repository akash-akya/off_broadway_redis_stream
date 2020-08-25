defmodule OffBroadwayRedisStream.RedixClient do
  @moduledoc false

  alias Broadway.{Message, Acknowledger}
  require Logger

  @behaviour OffBroadwayRedisStream.RedisClient
  @behaviour Acknowledger

  @impl true
  def init(opts) do
    with :ok <- validate(opts) do
      config = Map.new(opts)
      ack_ref = Broadway.TermStorage.put(config)
      {:ok, Map.merge(config, %{ack_ref: ack_ref, last_id: "0", min_idle_time: 30_000})}
    end
  end

  @max_messages 1000

  @impl true
  def receive_messages(demand, opts) do
    {cmd, opts} = xreadgroup(min(demand, @max_messages), opts)
    stream = opts.stream

    messages =
      case Redix.command(opts.redis_instance, cmd) do
        {:ok, [[^stream, messages]]} ->
          wrap_received_messages(messages, opts.ack_ref)

        {:ok, nil} ->
          []
      end

    {messages, opts}
  end

  @impl Acknowledger
  def ack(ack_ref, successful, failed) do
    opts = Broadway.TermStorage.get!(ack_ref)
    ack_messages(successful ++ failed, opts, :successful)
    :ok
  end

  @impl Acknowledger
  def configure(_ack_ref, ack_data, options) do
    ack_data = Map.merge(ack_data, Map.new(options))
    {:ok, ack_data}
  end

  @max_id round(:math.pow(2, 64)) - 1

  @impl true
  def heartbeat(%{consumer_group: group, consumer_name: name, stream: stream} = opts) do
    # we refresh idle time by attempting to read non-existent entry
    cmd = ~w(XREADGROUP GROUP #{group} #{name} COUNT 1 STREAMS #{stream} #{@max_id})

    case Redix.command(opts.redis_instance, cmd) do
      {:ok, [[^stream, []]]} -> :ok
      error -> error
    end
  end

  @impl true
  def consumers_info(opts) do
    cmd = ~w(XINFO consumers #{opts.stream} #{opts.consumer_group})

    case Redix.command(opts.redis_instance, cmd) do
      {:ok, info} -> {:ok, to_map(info)}
      error -> error
    end
  end

  @impl true
  def pending(opts, consumer, count) do
    cmd = ~w(XPENDING #{opts.stream} #{opts.consumer_group} - + #{count} #{consumer})

    case Redix.command(opts.redis_instance, cmd) do
      {:ok, res} -> {:ok, res}
      error -> error
    end
  end

  @impl true
  def claim(opts, idle, ids) do
    cmd = ["XCLAIM", opts.stream, opts.consumer_group, opts.consumer_name, idle] ++ ids

    case Redix.command(opts.redis_instance, cmd) do
      {:ok, nil} ->
        {:ok, []}

      {:ok, messages} ->
        {:ok, wrap_received_messages(messages, opts.ack_ref)}

      error ->
        error
    end
  end

  defp wrap_received_messages(messages, ack_ref) do
    Enum.map(messages, fn message ->
      acknowledger = build_acknowledger(message, ack_ref)
      %Message{data: message, acknowledger: acknowledger}
    end)
  end

  defp build_acknowledger([id, _], ack_ref) do
    {__MODULE__, ack_ref, %{receipt: %{id: id}}}
  end

  defp ack_messages([], _opts, _kind), do: :ok

  defp ack_messages(messages, opts, _kind) do
    messages
    |> apply_ack_func(opts)
    |> handle_acknowledged_messages()
  end

  defp apply_ack_func(messages, opts) do
    ids = Enum.map(messages, &extract_message_id/1)
    cmd = ["XACK", opts.stream, opts.consumer_group] ++ ids
    Redix.command(opts.redis_instance, cmd)
  end

  defp extract_message_id(message) do
    {_, _, %{receipt: %{id: id}}} = message.acknowledger
    id
  end

  defp handle_acknowledged_messages({:ok, _}), do: :ok

  defp handle_acknowledged_messages({:error, reason}) do
    Logger.error("Unable to acknowledge messages with Redis. Reason: #{inspect(reason)}")
    :ok
  end

  defp xreadgroup(count, opts) do
    cmd = [
      "XREADGROUP",
      "GROUP",
      opts.consumer_group,
      opts.consumer_name,
      "COUNT",
      count,
      "STREAMS",
      opts.stream,
      opts.last_id
    ]

    {cmd, %{opts | last_id: ">"}}
  end

  defp validate(opts) when is_list(opts) do
    with :ok <- validate_option(:redis_instance, opts[:redis_instance]),
         :ok <- validate_option(:stream, opts[:stream]),
         :ok <- validate_option(:consumer_group, opts[:consumer_group]),
         :ok <- validate_option(:consumer_name, opts[:consumer_name]) do
      :ok
    end
  end

  defp validate_option(:redis_instance, value) when not is_atom(value) or is_nil(value),
    do: validation_error(:redis_instance, "an atom", value)

  defp validate_option(:consumer_group, value) when not is_binary(value) or value == "",
    do: validation_error(:consumer_group, "a non empty string", value)

  defp validate_option(:consumer_name, value) when not is_binary(value) or value == "",
    do: validation_error(:consumer_name, "a non empty string", value)

  defp validate_option(:stream, value) when not is_binary(value) or value == "",
    do: validation_error(:stream, "a non empty string", value)

  defp validate_option(_, _), do: :ok

  defp validation_error(option, expected, value) do
    {:error, "expected #{inspect(option)} to be #{expected}, got: #{inspect(value)}"}
  end

  defp to_map(info), do: to_map(info, [])

  defp to_map([], [{_key, _value} | _] = acc), do: Map.new(acc)

  defp to_map([], acc), do: Enum.reverse(acc)

  defp to_map([key, value | rest], acc) when is_binary(key) do
    to_map(rest, [{key, to_map(value)} | acc])
  end

  defp to_map([info | rest], acc) when is_list(info) do
    to_map(rest, [to_map(info) | acc])
  end

  defp to_map([info | rest], acc) do
    to_map(rest, [info | acc])
  end

  defp to_map(info, _acc), do: info
end
