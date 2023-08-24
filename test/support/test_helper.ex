defmodule TestHelper do
  def stop_process(pid) do
    Process.flag(:trap_exit, true)

    ref = Process.monitor(pid)
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end

  def redix_opts, do: [host: host(), port: port()]

  defp host do
    System.get_env("REDIS_HOST") || "localhost"
  end

  defp port do
    if p = System.get_env("REDIS_PORT") do
      {port, ""} = Integer.parse(p)
      port
    else
      6379
    end
  end

end