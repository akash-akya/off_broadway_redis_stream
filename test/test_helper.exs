host = System.get_env("REDIS_HOST") || "localhost"

port =
  if p = System.get_env("REDIS_PORT") do
    {port, ""} = Integer.parse(p)
    port
  else
    6379
  end

{:ok, _} = Redix.start_link(host: host, port: port, name: :redix)

ExUnit.start()
