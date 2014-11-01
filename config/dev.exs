use Mix.Config

config :logger,
  level: :info

config :logger, :console,
  format: "$date $time [$level] $metadata$message\n",
  metadata: [:user_id]
