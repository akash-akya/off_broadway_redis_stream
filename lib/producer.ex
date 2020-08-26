defmodule OffBroadwayRedisStream.Producer do
  @moduledoc """
  A GenStage Producer for Redis Stream.
  Acts as a unique consumer in specified Consumer Group. see https://redis.io/topics/streams-intro.

  Producer automatically claim pending messages after certain timeout in case of abnormal termination of a producer.

  ## Producer Options

    * `:redis_instance` - Required. Redix instance must started separately and name of that instance needs to be passed. For more infromation see [Redix Documentation](https://hexdocs.pm/redix/Redix.html#start_link/1)

    * `:receive_interval` - Optional. The duration (in milliseconds) for which the producer
      waits before making a request for more messages if there are no events in stream. Default is 2000.

    * `:stream` - Required. Redis stream name

    * `:consumer_group` - Required. Redis consumer group

    * `:consumer_name` - Required. Redis Consumer name for the producer

    * `:heartbeat` - Optional. Producer sends heartbeats at regular intervals, interval duration. Default is 5000

    * `:allowed_missed_heartbeats` - Optional. Missed heartbeats allowed, after this that consumer is considered to be dead and other consumers claim its pending events. Default is 4

  ## Acknowledgments

  Both successful and failed messages are acknowledged. use `handle_failure` callback to handle failures such as moving to other stream or persisting failure job etc

  ## Message Data

  Message data is a 2 element list. First item is id of the message, second is the data
  """

  use GenStage
  alias Broadway.Producer
  @behaviour Producer

  @default_receive_interval 2000
  @default_allowed_missed_heartbeats 4
  @default_heartbeat_time 5000

  @impl GenStage
  def init(opts) do
    client = opts[:client] || OffBroadwayRedisStream.RedixClient
    receive_interval = opts[:receive_interval] || @default_receive_interval
    heartbeat_time = opts[:heartbeat_time] || @default_heartbeat_time

    allowed_missed_heartbeats =
      opts[:allowed_missed_heartbeats] || @default_allowed_missed_heartbeats

    case client.init(opts) do
      {:error, message} ->
        raise ArgumentError, "invalid options given to #{inspect(client)}.init/1, " <> message

      {:ok, redis_state} ->
        {:ok, heartbeat} =
          OffBroadwayRedisStream.Heartbeat.start_link(client, redis_state, heartbeat_time)

        {:producer,
         %{
           demand: 0,
           redis_client: client,
           redis_state: redis_state,
           receive_interval: receive_interval,
           heartbeat: heartbeat,
           receive_timer: nil,
           heartbeat_time: heartbeat_time,
           allowed_missed_heartbeats: allowed_missed_heartbeats,
           last_checked: 0
         }}
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
  def handle_info(_, state) do
    {:noreply, [], state}
  end

  @impl Producer
  def prepare_for_draining(%{receive_timer: receive_timer} = state) do
    receive_timer && Process.cancel_timer(receive_timer)
    {:noreply, [], %{state | receive_timer: nil}}
  end

  defp receive_messages(%{receive_timer: nil, demand: demand} = state) when demand > 0 do
    {claimed_messages, state} = maybe_claim_dead_consumer_messages(state)
    {new_messages, state} = fetch_messages(state)
    messages = Enum.concat([claimed_messages, new_messages])

    receive_timer = maybe_scheduler_timer(state, messages, state.demand)
    state = %{state | receive_timer: receive_timer}

    {:noreply, messages, state}
  end

  defp receive_messages(state) do
    {:noreply, [], state}
  end

  defp maybe_scheduler_timer(state, messages, demand) do
    case {messages, demand} do
      {[], _} -> schedule_receive_messages(state.receive_interval)
      {_, 0} -> nil
      _ -> schedule_receive_messages(0)
    end
  end

  defp schedule_receive_messages(interval) do
    Process.send_after(self(), :receive_messages, interval)
  end

  defp maybe_claim_dead_consumer_messages(state) do
    now = DateTime.utc_now() |> DateTime.to_unix()
    expire_time = state.allowed_missed_heartbeats * state.heartbeat_time

    if now - state.last_checked > expire_time do
      {messages, state} = claim_dead_consumer_messages(state)

      if length(messages) >= 0 do
        {messages, state}
      else
        {[], %{state | last_checked: now}}
      end
    else
      {[], state}
    end
  end

  defp claim_dead_consumer_messages(state, acc \\ []) do
    {:ok, consumers} = consumers_info(state)
    expire_time = state.allowed_missed_heartbeats * state.heartbeat_time

    {status, messages} =
      consumers
      |> dead_consumers(expire_time)
      |> claim_consumers(state)

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
    |> Enum.filter(fn consumer ->
      consumer["pending"] > 0 && consumer["idle"] > expire_time
    end)
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

  defp claim_consumer(%{redis_client: client, redis_state: redis_state}, consumer, demand) do
    count = min(consumer["pending"], demand)

    {:ok, pending_messages} = client.pending(redis_state, consumer["name"], count)
    ids = Enum.map(pending_messages, &Enum.at(&1, 0))
    {:ok, messages} = client.claim(redis_state, consumer["idle"], ids)

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

  defp fetch_messages(%{demand: demand, redis_client: client, redis_state: redis_state} = state)
       when demand > 0 do
    {messages, redis_state} = client.receive_messages(state.demand, redis_state)
    {messages, %{state | redis_state: redis_state, demand: state.demand - length(messages)}}
  end

  defp fetch_messages(state), do: {[], state}

  defp consumers_info(%{redis_client: client, redis_state: redis_state}) do
    client.consumers_info(redis_state)
  end
end
