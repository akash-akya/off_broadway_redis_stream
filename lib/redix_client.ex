defmodule OffBroadwayRedisStream.RedixClient do
  @moduledoc false

  alias Broadway.{Message, Acknowledger}

  @behaviour OffBroadwayRedisStream.RedisClient
  @behaviour Acknowledger

  @impl true
  def init(opts) do
    with :ok <- validate(opts) do
      config = Map.new(opts)
      ack_ref = Broadway.TermStorage.put(config)
      {:ok, Map.put(config, :ack_ref, ack_ref)}
    end
  end

  @timeout 5 * 60 * 1000
  @max_messages 1000

  @impl true
  def receive_messages(demand, opts) do
    cmd = xreadgroup(min(demand, @max_messages), @timeout, opts)
    stream = opts.stream

    case Redix.command(opts.redis_instance, cmd, timeout: @timeout + 1_000) do
      {:ok, [[^stream, messages]]} -> wrap_received_messages(messages, opts.ack_ref)
      {:ok, nil} -> []
    end
  end

  @impl Acknowledger
  def ack(ack_ref, successful, _failed) do
    opts = Broadway.TermStorage.get!(ack_ref)

    successful
    |> Enum.chunk_every(@max_messages)
    |> Enum.each(&ack_messages(&1, opts))
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

  defp ack_messages(messages, opts) do
    Enum.map(messages, &extract_message_id/1)
    |> send_ack(opts)
  end

  defp extract_message_id(message) do
    {_, _, %{receipt: %{id: id}}} = message.acknowledger
    id
  end

  defp send_ack(ids, opts) do
    cmd = ["XACK", opts.stream, opts.consumer_group] ++ ids
    expected_count = length(ids)

    case Redix.command(opts.redis_instance, cmd, timeout: @timeout + 2_000) do
      {:ok, ^expected_count} -> :ok
    end
  end

  defp xreadgroup(count, timeout, %{consumer_group: group, consumer_name: name, stream: stream}) do
    ["XREADGROUP", "GROUP", group, name, "BLOCK", timeout, "COUNT", count, "STREAMS", stream, ">"]
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
end
