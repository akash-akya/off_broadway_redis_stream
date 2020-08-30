defmodule OffBroadwayRedisStream.RedixClient do
  @moduledoc false
  require Logger

  @behaviour OffBroadwayRedisStream.RedisClient

  @impl true
  def init(config) do
    config = Map.new(config)

    with :ok <- check_redis_version(config) do
      {:ok, config}
    end
  end

  @impl true
  def fetch(demand, last_id, config) do
    %{stream: stream, group: group, consumer_name: consumer_name, redis_instance: pid} = config

    cmd =
      ~w(XREADGROUP GROUP #{group} #{consumer_name} COUNT #{demand} STREAMS #{stream} #{last_id})

    case Redix.command(pid, cmd) do
      {:ok, [[^stream, messages]]} -> {:ok, messages}
      {:ok, nil} -> {:ok, []}
      error -> error
    end
  end

  @impl true
  def consumers_info(config) do
    %{stream: stream, group: group, redis_instance: pid} = config
    cmd = ~w(XINFO consumers #{stream} #{group})

    case Redix.command(pid, cmd) do
      {:ok, info} -> {:ok, to_map(info)}
      error -> error
    end
  end

  @impl true
  def pending(consumer, count, config) do
    %{stream: stream, group: group, redis_instance: pid} = config
    cmd = ~w(XPENDING #{stream} #{group} - + #{count} #{consumer})

    case Redix.command(pid, cmd) do
      {:ok, res} -> {:ok, res}
      error -> error
    end
  end

  @impl true
  def claim(idle, ids, config) do
    %{stream: stream, group: group, consumer_name: consumer_name, redis_instance: pid} = config
    cmd = ["XCLAIM", stream, group, consumer_name, idle] ++ ids

    case Redix.command(pid, cmd) do
      {:ok, nil} -> {:ok, []}
      {:ok, messages} -> {:ok, messages}
      error -> error
    end
  end

  @impl true
  def ack(ids, config) do
    %{stream: stream, group: group, redis_instance: pid} = config
    cmd = ["XACK", stream, group] ++ ids

    case Redix.command(pid, cmd) do
      {:ok, _} -> :ok
      error -> error
    end
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

  defp check_redis_version(config) do
    with {:ok, info} <- info(config) do
      if Version.compare(info["redis_version"], "6.0.0") in [:gt, :eq] do
        :ok
      else
        {:error, "only supports Redis version >= 6"}
      end
    end
  end

  defp info(config) do
    with {:ok, info} <- Redix.command(config.redis_instance, ~w(INFO server)) do
      info =
        String.split(info, "\n", trim: true)
        |> Enum.reject(&String.starts_with?(&1, "#"))
        |> Map.new(fn entry ->
          [key, value] = String.split(entry, ":", parts: 2, trim: true)
          {key, String.trim(value)}
        end)

      {:ok, info}
    end
  end
end
