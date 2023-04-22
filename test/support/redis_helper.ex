defmodule RedisHelper do
  @moduledoc false

  def xadd(pid, stream, id, list) do
    value = Enum.map_join(list, " ", fn {k, v} -> "#{k} #{inspect(v)}" end)
    Redix.command!(pid, ~w(XADD #{stream} #{id} #{value}))
  end

  def create_stream(pid, stream, group) do
    Redix.command!(pid, ~w(XGROUP CREATE #{stream} #{group} $ MKSTREAM))
  end

  def xpending(pid, stream, group, consumer) do
    Redix.command!(pid, ~w(XPENDING #{stream} #{group} - + 100000 #{consumer}))
  end

  def xlen(pid, stream) do
    Redix.command!(pid, ~w(XLEN #{stream}))
  end

  def flushall(pid) do
    Redix.command!(pid, ~w(FLUSHALL))
  end
end
