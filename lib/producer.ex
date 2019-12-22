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

  ## Acknowledgments

  In case of successful processing, the message is properly acknowledge to Redis Consumer Group.
  In case of failures, no message is acknowledged, which means Message will wont be removed from pending entries list (PEL). As of now consumer have to handle failure scenario to clean up pending entries. For more information, see: [Recovering from permanent failures](https://redis.io/topics/streams-intro#recovering-from-permanent-failures)

  ## Message Data

  Message data is a 2 element list. First item is id of the message, second is the data
  """

  use GenStage
  alias Broadway.Producer
  @behaviour Producer

  @impl GenStage
  def init(opts) do
    client = opts[:client] || OffBroadwayRedisStream.RedixClient

    case client.init(opts) do
      {:error, message} ->
        raise ArgumentError, "invalid options given to #{inspect(client)}.init/1, " <> message

      {:ok, opts} ->
        {:producer, %{demand: 0, redis_client: {client, opts}}}
    end
  end

  @impl GenStage
  def handle_demand(demand, state) do
    receive_messages(%{state | demand: state.demand + demand})
  end

  @impl GenStage
  def handle_info(:receive_messages, state) do
    receive_messages(state)
  end

  defp receive_messages(%{demand: demand, redis_client: {client, opts}} = state)
       when demand > 0 do
    messages = client.receive_messages(state.demand, opts)
    new_demand = demand - length(messages)

    case {messages, new_demand} do
      {[], _} -> receive_messages(state)
      {_, 0} -> nil
      _ -> receive_more_messages()
    end

    {:noreply, messages, %{state | demand: new_demand}}
  end

  defp receive_messages(state) do
    {:noreply, [], state}
  end

  defp receive_more_messages() do
    GenStage.async_info(self(), :receive_messages)
  end
end
