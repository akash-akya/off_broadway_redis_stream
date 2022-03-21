defmodule OffBroadwayRedisStream.MixProject do
  use Mix.Project

  @version "0.3.0"
  @scm_url "https://github.com/akash-akya/off_broadway_redis_stream"

  def project do
    [
      app: :off_broadway_redis_stream,
      version: @version,
      elixir: "~> 1.7",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),

      # Package
      package: package(),
      description: description(),

      # Docs
      source_url: @scm_url,
      homepage_url: @scm_url,
      docs: [
        main: "readme",
        source_ref: "v#{@version}",
        extras: [
          "README.md",
          "LICENSE.md"
        ]
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp description do
    "A Redis Stream consumer for Broadway"
  end

  defp package do
    [
      maintainers: ["Akash Hiremath"],
      licenses: ["Apache-2.0"],
      links: %{GitHub: @scm_url}
    ]
  end

  defp deps do
    [
      {:broadway, "~> 1.0 or ~> 0.6"},
      {:redix, ">= 0.0.0"},
      {:mox, "~> 1.0", only: :test},
      {:ex_doc, "~> 0.25", only: :dev, runtime: false}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
