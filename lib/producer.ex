defmodule OffBroadwayRedisStream.Producer do
  @moduledoc """
  A GenStage Producer for Redis Stream.
  Acts as a unique consumer in specified Consumer Group. see https://redis.io/topics/streams-intro.

  Note: Successfully handled messaged are acknowledged automatically. Failure needs to be handled by the consumer
  by explicitly.

  ## Options for `OffBroadwayRedisStream.RedixClient`

    * `:redis_instance` - Required. Redix instance must started separately and name of that instance needs to be passed. For more infromation see [Redix Documentation](https://hexdocs.pm/redix/Redix.html#start_link/1)

    * `:stream` - Required. Redis stream name

    * `:consumer_group` - Required. Redis consumer group

    * `:consumer_name` - Required. Redis Consumer name for the producer

  ## Producer Options

  These options applies to all producers, regardless of client implementation:

    * `:client` - Optional. A module that implements the `OffBroadwayRedisStream.RedisClient`
      behaviour. This module is responsible for fetching and acknowledging the
      messages. Pay attention that all options passed to the producer will be forwarded
      to the client. It's up to the client to normalize the options it needs. Default
      is `OffBroadwayRedisStream.RedixClient`.

    * `:receive_interval` - Optional. The duration (in milliseconds) for which the producer
      waits before making a request for more messages. Default is 5000.

  ## Acknowledgments

  Both successful and failed messages are acknowledged. use `handle_failure` callback to handle failures such as moving to other stream or persisting failure job etc

  ## Message Data

  Message data is a 2 element list. First item is id of the message, second is the data
  """

  use GenStage
  alias Broadway.Producer
  @behaviour Producer

  @default_receive_interval 5000

  @impl GenStage
  def init(opts) do
    client = opts[:client] || OffBroadwayRedisStream.RedixClient
    receive_interval = opts[:receive_interval] || @default_receive_interval

    case client.init(opts) do
      {:error, message} ->
        raise ArgumentError, "invalid options given to #{inspect(client)}.init/1, " <> message

      {:ok, redis_opts} ->
        redis_client = {client, redis_opts}

        {:ok, watcher} =
          OffBroadwayRedisStream.Watcher.start_link(redis_client, opts[:heartbeat_time])

        {:producer,
         %{
           demand: 0,
           redis_client: redis_client,
           receive_timer: nil,
           receive_interval: receive_interval,
           watcher: watcher
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
    {client, opts} = state.redis_client
    {messages, opts} = client.receive_messages(state.demand, opts)
    new_demand = demand - length(messages)

    receive_timer =
      case {messages, new_demand} do
        {[], _} -> schedule_receive_messages(state.receive_interval)
        {_, 0} -> nil
        _ -> schedule_receive_messages(0)
      end

    state = %{
      state
      | demand: new_demand,
        receive_timer: receive_timer,
        redis_client: {client, opts}
    }

    {:noreply, messages, state}
  end

  defp receive_messages(state) do
    {:noreply, [], state}
  end

  defp schedule_receive_messages(interval) do
    Process.send_after(self(), :receive_messages, interval)
  end
end
