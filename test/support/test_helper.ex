defmodule TestHelper do
  @moduledoc false
  def redix_opts, do: [host: host(), port: port()]

  def host do
    System.get_env("REDIS_HOST") || "localhost"
  end

  def port do
    if p = System.get_env("REDIS_PORT") do
      {port, ""} = Integer.parse(p)
      port
    else
      6379
    end
  end
end
