defmodule OffBroadwayRedisStream.RedixClient do
  @moduledoc false

  alias Broadway.{Message, Acknowledger}
  require Logger

  @behaviour OffBroadwayRedisStream.RedisClient
  @behaviour Acknowledger

  @impl true
  def init(opts) do
    opts = Keyword.merge([on_failure: :ack], opts)

    with :ok <- validate(opts) do
      config = Map.new(opts)
      ack_ref = Broadway.TermStorage.put(config)
      {:ok, Map.put(config, :ack_ref, ack_ref)}
    end
  end

  @max_messages 1000

  @impl true
  def receive_messages(demand, opts) do
    cmd = xreadgroup(min(demand, @max_messages), opts)
    stream = opts.stream

    case Redix.command(opts.redis_instance, cmd) do
      {:ok, [[^stream, messages]]} ->
        wrap_received_messages(messages, opts.ack_ref, opts.on_failure)

      {:ok, nil} ->
        []
    end
  end

  @impl Acknowledger
  def ack(ack_ref, successful, failed) do
    opts = Broadway.TermStorage.get!(ack_ref)
    ack_messages(successful, opts, :successful)
    ack_messages(failed, opts, :failed)
  end

  @impl Acknowledger
  def configure(_ack_ref, ack_data, options) do
    options = assert_valid_failure_opts!(options)
    ack_data = Map.merge(ack_data, Map.new(options))
    {:ok, ack_data}
  end

  defp assert_valid_failure_opts!(options) do
    Enum.map(options, fn
      {:on_failure, value} ->
        case validate_option(:on_failure, value) do
          :ok ->
            {:on_failure, value}

          {:error, reason} ->
            raise ArgumentError, reason
        end

      {other, _value} ->
        raise ArgumentError, "unsupported configure option #{inspect(other)}"
    end)
  end

  defp wrap_received_messages(messages, ack_ref, on_failure) do
    Enum.map(messages, fn message ->
      acknowledger = build_acknowledger(message, ack_ref, on_failure)
      %Message{data: message, acknowledger: acknowledger}
    end)
  end

  defp build_acknowledger([id, _], ack_ref, on_failure) do
    {__MODULE__, ack_ref, %{receipt: %{id: id, on_failure: on_failure}}}
  end

  defp ack_messages([], _opts, _kind), do: :ok

  defp ack_messages(messages, opts, kind) do
    messages
    |> Enum.group_by(&group_acknowledger(&1, kind))
    |> Enum.map(fn {action, messages} ->
      action |> apply_ack_func(messages, opts) |> handle_acknowledged_messages()
    end)
  end

  defp group_acknowledger(%{acknowledger: {_mod, _chan, ack_data}}, kind) do
    ack_data_action(ack_data, kind)
  end

  defp ack_data_action(_, :successful), do: :ack
  defp ack_data_action(%{receipt: %{on_failure: action}}, :failed), do: action

  defp apply_ack_func(:ignore, _messages, _opts), do: :ok

  defp apply_ack_func(:ack, messages, opts) do
    ids = Enum.map(messages, &extract_message_id/1)
    cmd = ["XACK", opts.stream, opts.consumer_group] ++ ids
    Redix.command(opts.redis_instance, cmd)
  end

  defp extract_message_id(message) do
    {_, _, %{receipt: %{id: id}}} = message.acknowledger
    id
  end

  defp handle_acknowledged_messages(:ok), do: :ok

  defp handle_acknowledged_messages({:ok, _}), do: :ok

  defp handle_acknowledged_messages({:error, reason}) do
    Logger.error("Unable to acknowledge messages with Redis. Reason: #{inspect(reason)}")
    :ok
  end

  defp xreadgroup(count, %{consumer_group: group, consumer_name: name, stream: stream}) do
    ["XREADGROUP", "GROUP", group, name, "COUNT", count, "STREAMS", stream, ">"]
  end

  defp validate(opts) when is_list(opts) do
    with :ok <- validate_option(:redis_instance, opts[:redis_instance]),
         :ok <- validate_option(:stream, opts[:stream]),
         :ok <- validate_option(:consumer_group, opts[:consumer_group]),
         :ok <- validate_option(:consumer_name, opts[:consumer_name]),
         :ok <- validate_option(:on_failure, opts[:on_failure] || :ignore) do
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

  defp validate_option(:on_failure, value) when value not in [:ack, :ignore],
    do: validation_error(:on_failure, "a valid :on_failure value", value)

  defp validate_option(_, _), do: :ok

  defp validation_error(option, expected, value) do
    {:error, "expected #{inspect(option)} to be #{expected}, got: #{inspect(value)}"}
  end
end
