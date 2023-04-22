defmodule OffBroadwayRedisStream.Producer do
  @moduledoc """
  A GenStage Producer for Redis Stream.

  Broadway producer acts as a consumer in the specified Redis stream consumer group. You can run multiple consumers to get better throughput and fault tolerance. Please check [Redis Stream Intro](https://redis.io/topics/streams-intro) for details on stream data type.

  It supports failover by automatically claiming pending messages when a node dies. A node is considered dead when it fails to send heartbeats.

  Currently, it only supports Redis 6.0.2 and above

  ## Producer Options

    * `:redis_client_opts` - Required. Redis client specific options. Default client is [Redix](https://hexdocs.pm/redix/Redix.html) and for Redix this is used to start redix process `Redix.start_link(opts)`. see [Redix Documentation](https://hexdocs.pm/redix/Redix.html#start_link/1)

    * `:receive_interval` - Optional. The duration (in milliseconds) for which the producer
      waits before making a request for more messages if there are no events in stream. Default is `1000`.

    * `:stream` - Required. Redis stream name

    * `:group` - Required. Redis consumer group. Group will be created with `:group_start_id` ID if it is not already present.

    * `:group_start_id` - Optional. Starting stream ID which should be used when consumer group *created*. Use $ for latest ID. see [XGROUP CREATE](https://redis.io/commands/xgroup). Default is `$`

    * `:consumer_name` - Required. Redis consumer name for the Broadway instance in the consumer-group. If you are running multiple consumers, make sure that each consumer has unique name.

    * `:heartbeat_interval` - Optional. Producer sends heartbeats at regular intervals, this is interval duration. Default is `5000`

    * `:allowed_missed_heartbeats` - Optional. Number of allowed missing heartbeats for a consumer. The consumer is considered to be dead after this and other consumers claim its pending messages. Default is `3`

    * `:make_stream` - Optional. Appends MKSTREAM subcommand to `XGROUP CREATE` which automatically create the stream if it doesn't exist. See [XGROUP CREATE](https://redis.io/commands/xgroup). Default is `false`

    * `delete_on_acknowledgment` - Optional. When `XACK`ing a message also
      `XDEL`ete it. Defaults to `false`

  ## Acknowledgments

  Both successful and failed messages are acknowledged by default. Use `Broadway.Message.configure_ack/2` to change this behaviour for failed messages. If a message configured to retry, that message will be attempted again in next batch.
  ```elixir
  if message.metadata.attempt < @max_attempts do
    Message.configure_ack(message, retry: true)
  else
    message
  end
  ```
  `attempt` field in metadata can be used to control maximum retries.
  use `handle_failure` callback to handle failures by moving messages to other stream or persisting failed jobs etc

  ## Message Data

  Message data is a 2 element list. First item is id of the message, second is
  the data

  ## Retry After

  If `message.metadata.retry_after` is present and represents a unix
  timestamp (in milliseconds) in the future then the message will not be retried until after the
  time has passed. This is useful for when you are using
  `off_broadway_redis_stream` to talk to third party apis that may rate limit
  you and you need to back off from sending requests.
  """

  use GenStage

  alias Broadway.Message
  alias Broadway.Producer
  alias OffBroadwayRedisStream.Acknowledger
  alias OffBroadwayRedisStream.Heartbeat
  alias OffBroadwayRedisStream.RedisClient

  require Logger

  @behaviour Producer

  @default_opts [
    heartbeat_interval: 5000,
    receive_interval: 1000,
    client: OffBroadwayRedisStream.RedixClient,
    allowed_missed_heartbeats: 3,
    max_pending_ack: 100_000,
    redis_command_retry_timeout: 300,
    group_start_id: "$",
    make_stream: false,
    delete_on_acknowledgment: false
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
            pending_ack: [],
            retryable: []
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
  def handle_info({:ack, ack_ids, retryable}, %{delete_on_acknowledgment: true} = state) do
    ids = state.pending_ack ++ ack_ids
    state = %{state | retryable: state.retryable ++ retryable}

    case ack_and_delete_messages(ids, state) do
      :ok ->
        {:noreply, [], %{state | pending_ack: []}}

      {:error, error} ->
        Logger.warn(
          "Unable to acknowledge and delete messages with Redis. Reason: #{inspect(error)}"
        )

        if length(ids) > state.max_pending_ack do
          {:stop, "Pending ack count is more than maximum limit #{state.max_pending_ack}", state}
        else
          {:noreply, [], %{state | pending_ack: ids}}
        end
    end
  end

  @impl GenStage
  def handle_info({:ack, ack_ids, retryable}, state) do
    ids = state.pending_ack ++ ack_ids
    state = %{state | retryable: state.retryable ++ retryable}

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

  @impl true
  def terminate(_reason, %{delete_on_acknowledgment: true} = state) do
    case ack_and_delete_messages(state.pending_ack, state, 2) do
      :ok ->
        :ok

      {:error, error} ->
        Logger.warn(
          "Unable to acknowledge and delete messages with Redis. Reason: #{inspect(error)}"
        )
    end

    Heartbeat.stop(state.heartbeat_pid)
    :ok
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

  defp ack_and_delete_messages(ids, state, retry_count \\ 0) do
    with :ok <- redis_cmd(:ack, [ids], state, retry_count),
         :ok <- redis_cmd(:delete_message, [ids], state, retry_count) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp receive_messages(%{receive_timer: nil, demand: demand} = state) when demand > 0 do
    {retryable_messages, state} = retryable_messages(state)
    state = %{state | demand: state.demand - length(retryable_messages)}

    {claimed_messages, last_checked} = maybe_claim_dead_consumer_messages(state)
    state = %{state | demand: state.demand - length(claimed_messages), last_checked: last_checked}

    {new_messages, last_id} = fetch_messages_from_redis(state)
    state = %{state | demand: state.demand - length(new_messages), last_id: last_id}

    messages = retryable_messages ++ claimed_messages ++ new_messages
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

  @max_messages_per_batch 100_000

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

    count = min(demand, @max_messages_per_batch)

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

  defp retryable_messages(state) do
    %{demand: demand, retryable: retryable} = state

    current_time = System.os_time(:millisecond)

    {retry_now, retry_later} =
      Enum.split_with(retryable, fn
        %{metadata: %{retry_after: retry_after}} ->
          retry_after < current_time

        _ ->
          true
      end)

    {messages_to_retry_now, rest} = Enum.split(retry_now, demand)

    {prepare_failed_messages(messages_to_retry_now), %{state | retryable: retry_later ++ rest}}
  end

  defp wrap_messages(redis_messages, stream, group) do
    Enum.map(redis_messages, fn [id, _] = data ->
      ack_data = %{id: id, retry: false}
      ack_ref = {self(), {stream, group}}

      %Message{
        data: data,
        metadata: %{id: id, attempt: 1},
        acknowledger: {Acknowledger, ack_ref, ack_data}
      }
    end)
  end

  defp prepare_failed_messages(messages) do
    Enum.map(messages, fn message ->
      {_, ack_ref, ack_data} = message.acknowledger

      metadata =
        message.metadata
        |> Map.update!(:attempt, &(&1 + 1))
        |> Map.delete(:retry_after)

      %Message{
        message
        | metadata: metadata,
          acknowledger: {Acknowledger, ack_ref, %{ack_data | retry: false}}
      }
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
         :ok <- validate_option(:heartbeat_interval, opts[:heartbeat_interval]),
         :ok <- validate_option(:group_start_id, opts[:group_start_id]),
         :ok <- validate_option(:redis_command_retry_timeout, opts[:redis_command_retry_timeout]),
         :ok <- validate_option(:make_stream, opts[:make_stream]) do
      validate_option(:delete_on_acknowledgment, opts[:delete_on_acknowledgment])
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

  defp validate_option(:make_stream, value) when not is_boolean(value),
    do: validation_error(:make_stream, "a boolean", value)

  defp validate_option(:delete_on_acknowledgment, value) when not is_boolean(value),
    do: validation_error(:delete_on_acknowledgment, "a boolean", value)

  defp validate_option(_, _), do: :ok

  defp validation_error(option, expected, value) do
    {:error, "expected #{inspect(option)} to be #{expected}, got: #{inspect(value)}"}
  end
end
