defmodule Diver.JavaServer do
  use GenServer

  require Logger

  defstruct [node: nil, port: nil]
  @type t :: %__MODULE__{node: String.t, port: port()}

  @registered_proc_name :diver_java_server

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, [])
  end

  def init(args), do: init(args, System.find_executable("java"))

  defp init(_args, nil) do
    {:stop, "Cannot locate Java executable on PATH environment variable."}
  end
  defp init(args, exe) do
    self_node = Atom.to_string(Kernel.node())
    java_node = "__diver__" <> self_node
    cookie = Node.get_cookie()
    jarfile = :code.priv_dir(:diver) ++ '/diver-0.1.0-SNAPSHOT.jar'
    jvm_args = [
      '-jar', jarfile, self_node, java_node, cookie, @registered_proc_name] ++ Keyword.values(args)
    port = Port.open(
      {:spawn_executable, exe},
      [{:line, 1000},
       :stderr_to_stdout,
       {:args, jvm_args}])
    state = Kernel.struct(__MODULE__, [node: String.to_atom(java_node), port: port])
    sync_with_java_startup(state)
  end

  defp sync_with_java_startup(state) do
    port = state.port
    receive do
      {^port, {:data, {:eol, 'READY'}}} ->
        Logger.info("Successfully started Java server process.")
        process = receive_pid(state)
        true = Process.link(process)
        Logger.info("Java server process now linked.")
        true = Node.monitor(state.node, true)
        {:ok, state}
      {^port, {:data, {:eol, stdout}}} ->
        {:stop, stdout}
      msg ->
        {:stop, msg}
    end
  end

  defp receive_pid(state) do
    Kernel.send({@registered_proc_name, state.node}, {:pid, Kernel.self()})
    receive do
      {:pid, pid} -> pid
    after
      5000 -> raise "Request for Java server pid timed out."
    end
  end

  def handle_info({:nodedown, node}, %__MODULE__{node: node} = state) do
    Logger.error("Java server process is down.")
    {:stop, :nodedown, state}
  end
  def handle_info({port, {:data, {:eol, msg}}}, %__MODULE__{port: port} = state) do
    Logger.info(msg)
    {:noreply, state}
  end
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  def terminate(_reason, state), do: Port.close(state.port); :ok
end
