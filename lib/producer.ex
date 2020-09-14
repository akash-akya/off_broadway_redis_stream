defmodule OffBroadwayRedisStream.Producer do
  @moduledoc """
  A GenStage Producer for Redis Stream.
  Acts as a unique consumer in specified Consumer Group. see https://redis.io/topics/streams-intro.

  Producer automatically claim pending messages after certain timeout in case of abnormal termination of a producer.

  Currently it only support Redis 6.0 and above

  ## Producer Options

    * `:redis_client_opts` - Required. Redis client specific options. Default client is [Redix](https://hexdocs.pm/redix/Redix.html) and for Redix this is used to start redix process `Redix.start_link(opts)`. see [Redix Documentation](https://hexdocs.pm/redix/Redix.html#start_link/1)

    * `:receive_interval` - Optional. The duration (in milliseconds) for which the producer
      waits before making a request for more messages if there are no events in stream. Default is 2000.

    * `:stream` - Required. Redis stream name

    * `:group` - Required. Redis consumer group. Group will be created with `:group_start_id` ID if it is not already present.

    * `:consumer_name` - Required. Redis Consumer name for the producer

    * `:group_start_id` - Optional. Starting stream ID which should be used when consumer group *created*. Use $ for latest ID. see [XGROUP CREATE](https://redis.io/commands/xgroup). Default is `$`

    * `:heartbeat_interval` - Optional. Producer sends heartbeats at regular intervals, interval duration. Default is 5000

    * `:allowed_missed_heartbeats` - Optional. Missed heartbeats allowed, after this that consumer is considered to be dead and other consumers claim its pending events. Default is 4

  ## Acknowledgments

  Both successful and failed messages are acknowledged. use `handle_failure` callback to handle failures such as moving to other stream or persisting failure job etc

  ## Message Data

  Message data is a 2 element list. First item is id of the message, second is the data
  """

  use GenStage
  alias Broadway.Producer
  alias Broadway.Message
  alias OffBroadwayRedisStream.Acknowledger
  alias OffBroadwayRedisStream.Heartbeat
  alias OffBroadwayRedisStream.RedisClient
  require Logger

  @behaviour Producer

  @default_opts [
    heartbeat_interval: 5000,
    receive_interval: 2000,
    client: OffBroadwayRedisStream.RedixClient,
    allowed_missed_heartbeats: 4,
    max_pending_ack: 1000,
    redis_command_retry_timeout: 500,
    group_start_id: "$"
  ]

  @impl GenStage
  def init(opts) do
    opts = Keyword.merge(@default_opts, opts)
    validate!(opts)
    client = opts[:client]

    case client.init(opts) do
      {:error, message} ->
        raise ArgumentError, "invalid options given to #{inspect(client)}.init/1, " <> message

      {:ok, redis_config} ->
        init_consumer_group!(client, opts[:group_start_id], redis_config)

        {:ok, heartbeat_pid} =
          Heartbeat.start_link(client, redis_config, opts[:heartbeat_interval])

        state =
          Map.new(opts)
          |> Map.merge(%{
            demand: 0,
            redis_client: client,
            redis_config: redis_config,
            receive_timer: nil,
            last_id: "0",
            last_checked: 0,
            heartbeat_pid: heartbeat_pid,
            pending_ack: []
          })

        {:producer, state}
    end
  end

  @impl GenStage
  def handle_demand(demand, state) do
    receive_messages(%{state | demand: state.demand + demand})
  end

  @impl GenStage
  def handle_info(:receive_messages, %{receive_timer: nil} = state) do
    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info(:receive_messages, state) do
    receive_messages(%{state | receive_timer: nil})
  end

  @impl GenStage
  def handle_info({:ack, ids}, state) do
    ids = state.pending_ack ++ ids

    case redis_cmd(:ack, [ids], state, 0) do
      :ok ->
        {:noreply, [], %{state | pending_ack: []}}

      {:error, error} ->
        Logger.warn("Unable to acknowledge messages with Redis. Reason: #{inspect(error)}")

        if length(ids) > state.max_pending_ack do
          {:stop, "Pending ack count is more than maximum limit #{state.max_pending_ack}", state}
        else
          {:noreply, [], %{state | pending_ack: ids}}
        end
    end
  end

  @impl GenStage
  def handle_info(_, state) do
    {:noreply, [], state}
  end

  @impl GenStage
  def terminate(_reason, state) do
    case redis_cmd(:ack, [state.pending_ack], state, 2) do
      :ok ->
        :ok

      {:error, error} ->
        Logger.warn("Unable to acknowledge messages with Redis. Reason: #{inspect(error)}")
    end

    Heartbeat.stop(state.heartbeat_pid)
    :ok
  end

  @impl Producer
  def prepare_for_draining(%{receive_timer: receive_timer} = state) do
    receive_timer && Process.cancel_timer(receive_timer)
    {:noreply, [], %{state | receive_timer: nil}}
  end

  defp receive_messages(%{receive_timer: nil, demand: demand} = state) when demand > 0 do
    {claimed_messages, last_checked} = maybe_claim_dead_consumer_messages(state)
    state = %{state | demand: state.demand - length(claimed_messages), last_checked: last_checked}

    {new_messages, last_id} = fetch_messages_from_redis(state)
    state = %{state | demand: state.demand - length(new_messages), last_id: last_id}

    messages = claimed_messages ++ new_messages
    receive_timer = maybe_schedule_timer(state, length(messages), state.demand)

    {:noreply, messages, %{state | receive_timer: receive_timer}}
  end

  defp receive_messages(state) do
    {:noreply, [], state}
  end

  defp maybe_schedule_timer(state, current, demand) do
    case {current, demand} do
      {0, _} -> schedule_receive_messages(state.receive_interval)
      {_, 0} -> nil
      _ -> schedule_receive_messages(0)
    end
  end

  defp schedule_receive_messages(interval) do
    Process.send_after(self(), :receive_messages, interval)
  end

  defp maybe_claim_dead_consumer_messages(state) do
    now = DateTime.utc_now() |> DateTime.to_unix(:millisecond)
    expire_time = state.allowed_missed_heartbeats * state.heartbeat_interval
    last_checked = state.last_checked

    if now - last_checked > expire_time do
      {redis_messages, state} = claim_dead_consumer_messages(state)

      if length(redis_messages) > 0 do
        %{stream: stream, group: group} = state
        {wrap_messages(redis_messages, stream, group), last_checked}
      else
        {[], now}
      end
    else
      {[], last_checked}
    end
  end

  defp claim_dead_consumer_messages(state, acc \\ []) do
    {:ok, consumers} = redis_cmd(:consumers_info, [], state)
    expire_time = state.allowed_missed_heartbeats * state.heartbeat_interval

    {dead_without_pending, dead_with_pending} = dead_consumers(consumers, expire_time)
    prune_consumers(dead_without_pending, state)

    {status, messages} = claim_consumers(dead_with_pending, state)

    messages = acc ++ messages
    state = %{state | demand: state.demand - length(messages)}

    case status do
      :ok -> {messages, state}
      # someone else consumed messages
      :reset -> claim_dead_consumer_messages(state, messages)
    end
  end

  defp dead_consumers(consumers, expire_time) do
    consumers
    |> Enum.filter(&(&1["idle"] > expire_time))
    |> Enum.reduce(
      {[], []},
      fn
        %{"pending" => 0} = consumer, {without_pending, with_pending} ->
          {[consumer | without_pending], with_pending}

        consumer, {without_pending, with_pending} ->
          {without_pending, [consumer | with_pending]}
      end
    )
  end

  defp claim_consumers(consumers, state) do
    consumers
    |> Enum.concat([:end])
    |> Enum.reduce_while(
      {[], state.demand},
      fn
        :end, {acc, _demand} ->
          {:halt, {:ok, acc}}

        consumer, {acc, demand} ->
          case claim_consumer(state, consumer, demand) do
            {:ok, messages} when length(messages) == demand ->
              {:halt, {:ok, acc ++ messages}}

            {:ok, messages} ->
              {:cont, {acc ++ messages, demand - length(messages)}}

            {:reset, messages} ->
              {:halt, {:reset, acc ++ messages}}
          end
      end
    )
  end

  defp claim_consumer(state, consumer, demand) do
    count = min(consumer["pending"], demand)

    {:ok, pending_messages} = redis_cmd(:pending, [consumer["name"], count], state)
    ids = Enum.map(pending_messages, &Enum.at(&1, 0))
    {:ok, messages} = redis_cmd(:claim, [consumer["idle"], ids], state)

    received = length(messages)

    cond do
      received == demand ->
        {:ok, messages}

      received != length(ids) ->
        # someone else consumed messages
        {:reset, messages}

      true ->
        {:ok, messages}
    end
  end

  @max_messages 1000

  defp fetch_messages_from_redis(%{demand: demand} = state) when demand == 0,
    do: {[], state.last_id}

  defp fetch_messages_from_redis(state) do
    %{
      demand: demand,
      stream: stream,
      group: group,
      consumer_name: consumer_name,
      last_id: last_id
    } = state

    count = min(demand, @max_messages)

    case redis_cmd(:fetch, [count, last_id], state) do
      {:ok, []} ->
        {[], ">"}

      {:ok, redis_messages} ->
        last_id =
          cond do
            last_id == ">" ->
              ">"

            length(redis_messages) < count ->
              ">"

            true ->
              [last_id, _] = List.last(redis_messages)
              last_id
          end

        {wrap_messages(redis_messages, stream, group), last_id}

      {:error, reason} ->
        raise "cannot fetch messages from Redis (stream=#{stream} group=#{group} " <>
                "consumer=#{consumer_name}). Reason: #{inspect(reason)}"
    end
  end

  defp wrap_messages(redis_messages, stream, group) do
    Enum.map(redis_messages, fn [id, _] = data ->
      ack_data = %{id: id}
      ack_ref = {self(), {stream, group}}

      %Message{data: data, metadata: %{id: id}, acknowledger: {Acknowledger, ack_ref, ack_data}}
    end)
  end

  @max_retries 2
  defp redis_cmd(func, args, state, max_retries \\ @max_retries, retry_count \\ 0) do
    %{redis_client: client, redis_config: redis_config} = state

    case apply(client, func, args ++ [redis_config]) do
      {:error, %RedisClient.ConnectionError{} = error} when retry_count < max_retries ->
        Logger.warn(
          "Failed to run #{func}, retry_count: #{retry_count}, reason: #{inspect(error.reason)}"
        )

        Process.sleep(state.redis_command_retry_timeout * (retry_count + 1))
        redis_cmd(func, args, state, max_retries, retry_count + 1)

      result ->
        result
    end
  end

  defp init_consumer_group!(client, group_start_id, redis_config) do
    :ok = client.create_group(group_start_id, redis_config)
  end

  defp prune_consumers([], _state), do: :ok

  defp prune_consumers(consumers, state) do
    %{redis_client: client, redis_config: redis_config} = state
    names = Enum.map(consumers, & &1["name"])
    _ = client.delete_consumers(names, redis_config)
  end

  defp validate!(opts) do
    case validate(opts) do
      :ok -> :ok
      {:error, error} -> raise ArgumentError, message: error
    end
  end

  defp validate(opts) when is_list(opts) do
    with :ok <- validate_option(:stream, opts[:stream]),
         :ok <- validate_option(:group, opts[:group]),
         :ok <- validate_option(:consumer_name, opts[:consumer_name]),
         :ok <- validate_option(:receive_interval, opts[:receive_interval]),
         :ok <- validate_option(:allowed_missed_heartbeats, opts[:allowed_missed_heartbeats]),
         :ok <- validate_option(:heartbeat_interval, opts[:heartbeat_interval]) do
      :ok
    end
  end

  defp validate_option(:group, value) when not is_binary(value) or value == "",
    do: validation_error(:group, "a non empty string", value)

  defp validate_option(:consumer_name, value) when not is_binary(value) or value == "",
    do: validation_error(:consumer_name, "a non empty string", value)

  defp validate_option(:stream, value) when not is_binary(value) or value == "",
    do: validation_error(:stream, "a non empty string", value)

  defp validate_option(:heartbeat_interval, value) when not is_integer(value) or value < 0,
    do: validation_error(:heartbeat_interval, "a positive integer", value)

  defp validate_option(:receive_interval, value) when not is_integer(value) or value < 0,
    do: validation_error(:receive_interval, "a positive integer", value)

  defp validate_option(:group_start_id, value) when not is_binary(value),
    do: validation_error(:group_start_id, "a redis stream id or $", value)

  defp validate_option(:allowed_missed_heartbeats, value)
       when not is_integer(value) and value > 0,
       do: validation_error(:allowed_missed_heartbeats, "a positive integer", value)

  defp validate_option(:redis_command_retry_timeout, value)
       when not is_integer(value) and value > 0,
       do: validation_error(:redis_command_retry_timeout, "a positive integer", value)

  defp validate_option(_, _), do: :ok

  defp validation_error(option, expected, value) do
    {:error, "expected #{inspect(option)} to be #{expected}, got: #{inspect(value)}"}
  end
end
