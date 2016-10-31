defmodule Diver do
  use Application

  # TODO should I define stop/1 to tell the JVM to shutdown?

  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    children = if Application.get_env(:diver, :zk) do
      [
        worker(Diver.JavaServer, [Application.get_env(:diver, :zk)])
      ]
    else
      []
    end

    opts = [strategy: :one_for_one, name: Diver.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
