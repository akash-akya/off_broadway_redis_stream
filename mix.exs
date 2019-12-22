defmodule OffBroadwayRedisStream.MixProject do
  use Mix.Project

  def project do
    [
      app: :off_broadway_redis_stream,
      version: "0.1.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:broadway, git: "https://github.com/plataformatec/broadway.git"},
      {:redix, ">= 0.0.0"}
    ]
  end
end
