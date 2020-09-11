defmodule OffBroadwayRedisStream.RedixClient do
  @moduledoc false
  require Logger

  @behaviour OffBroadwayRedisStream.RedisClient

  @impl true
  def init(config) do
    with :ok <- validate_redix_opts(config[:redis_client_opts]),
         {:ok, pid} <- Redix.start_link(config[:redis_client_opts]),
         config <- Map.put(Map.new(config), :redix_pid, pid),
         :ok <- check_redis_version(config) do
      {:ok, config}
    end
  end

  @impl true
  def fetch(demand, last_id, config) do
    %{stream: stream, group: group, consumer_name: consumer_name, redix_pid: pid} = config

    cmd =
      ~w(XREADGROUP GROUP #{group} #{consumer_name} COUNT #{demand} STREAMS #{stream} #{last_id})

    case command(pid, cmd) do
      {:ok, [[^stream, messages]]} -> {:ok, messages}
      {:ok, nil} -> {:ok, []}
      result -> result
    end
  end

  @impl true
  def consumers_info(config) do
    %{stream: stream, group: group, redix_pid: pid} = config
    cmd = ~w(XINFO consumers #{stream} #{group})

    case command(pid, cmd) do
      {:ok, info} -> {:ok, to_map(info)}
      result -> result
    end
  end

  @impl true
  def create_group(id, config) do
    %{stream: stream, group: group, redix_pid: pid} = config
    cmd = ~w(XGROUP CREATE #{stream} #{group} #{id})

    case command(pid, cmd) do
      {:ok, _} -> :ok
      {:error, %Redix.Error{message: "BUSYGROUP Consumer Group name already exists"}} -> :ok
      result -> result
    end
  end

  @impl true
  def pending(consumer, count, config) do
    %{stream: stream, group: group, redix_pid: pid} = config
    cmd = ~w(XPENDING #{stream} #{group} - + #{count} #{consumer})
    command(pid, cmd)
  end

  @impl true
  def claim(idle, ids, config) do
    %{stream: stream, group: group, consumer_name: consumer_name, redix_pid: pid} = config
    cmd = ["XCLAIM", stream, group, consumer_name, idle] ++ ids

    case command(pid, cmd) do
      {:ok, nil} -> {:ok, []}
      result -> result
    end
  end

  @impl true
  def ack(ids, config) do
    %{stream: stream, group: group, redix_pid: pid} = config
    cmd = ["XACK", stream, group] ++ ids

    case Redix.noreply_command(pid, cmd) do
      :ok -> :ok
      result -> result
    end
  end

  @impl true
  def delete_consumers(consumers, config) do
    %{stream: stream, group: group, redix_pid: pid} = config
    commands = Enum.map(consumers, &["XGROUP", "DELCONSUMER", stream, group, &1])

    case Redix.noreply_pipeline(pid, commands) do
      :ok -> :ok
      result -> result
    end
  end

  defp command(pid, cmd) do
    case Redix.command(pid, cmd) do
      {:error, %Redix.ConnectionError{reason: reason}} ->
        {:error, %OffBroadwayRedisStream.RedisClient.ConnectionError{reason: reason}}

      result ->
        result
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
    with {:ok, info} <- command(config.redix_pid, ~w(INFO server)) do
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

  defp validate_redix_opts(nil),
    do:
      {:error, "invalid :redis_client_opts opts for Redix, see Redix.start_link/1 documentation"}

  defp validate_redix_opts(_), do: :ok
end
