defmodule Diver.Client do
  require Logger

  # -export([server/0]).
  # -export([ensure_table_exists/1, ensure_table_family_exists/2]).
  # -export([get_config/1, set_config/2]).
  # -export([flush/0, prefetch_meta/1]).
  #
  # -export([get/2, get/3, get/4, scan/3, scan_sync/2]).
  # -export([put/5, compare_and_set/6, increment/4]).
  # -export([delete/2, delete/3, delete/4]).
  #
  # -define(CONNECT_TIMEOUT, (10 * 1000)).
  #
  # -type table() :: binary().
  # -type cf() :: binary().
  # -type rowkey() :: binary().
  # -type qualifier() :: binary().
  # -type value() :: binary().
  # -type ts() :: integer().
  #
  # -type hbase_tuples() :: [hbase_tuple()].
  # -type hbase_tuple() :: {cf(), rowkey(), qualifier(), value(), ts()}.
  #
  # -type config_key() :: flush_interval | increment_buffer_size.
  # -type scan_opts() :: [scan_opt()].
  # -type scan_opt() :: {num_rows, integer()}
  #     | {family, binary()}
  #     | {key_regexp, binary()}
  #     | {max_num_bytes, integer()}
  #     | {max_num_keyvalues, integer()}
  #     | {max_num_rows, integer()}
  #     | {max_timestamp, integer()}
  #     | {max_versions, integer()}
  #     | {qualifier, integer()}
  #     | {server_block_cache, integer()}
  #     | {start_key, binary()}
  #     | {stop_key, binary()}
  #     | {time_range, integer(), integer()}
  #     | {filter, filter_opts()}.
  #
  # -type filter_opts() :: [filter_opt()].
  # -type filter_opt() :: {column_prefix, binary()}
  #     | {column_range, binary(), binary()}
  #     | {first_key_only}
  #     | {fuzzy_row, [{binary(), binary()}]}
  #     | {key_only}
  #     | {key_regexp, binary()}.
  #
  # -type error() :: {error, binary(), binary()} | {error, atom()}.
  #
  # -spec server() -> {atom(), atom()}.
  # server() ->
  #     {ok, NodeName} = gen_server:call(?MODULE, nodename),
  #     NodeName.
  #
  # -spec ensure_table_exists(table()) -> ok | error().
  # ensure_table_exists(Table) ->
  #     gen_server:call(server(), {ensure_table_exists, Table}).
  #
  # -spec ensure_table_family_exists(table(), cf()) -> ok | error().
  # ensure_table_family_exists(Table, CF) ->
  #     gen_server:call(server(), {ensure_table_family_exists, Table, CF}).
  #
  # -spec get_config(config_key()) -> {ok, integer()} | error().
  # get_config(Option) ->
  #     gen_server:call(server(), {get_conf, Option}).
  #
  # -spec set_config(config_key(), integer()) -> {ok, integer()} | error().
  # set_config(Option, Value) ->
  #     gen_server:call(server(), {set_conf, Option, Value}).
  #
  # -spec flush() -> ok | error().
  # flush() ->
  #     gen_server:call(server(), {flush}).
  #
  # -spec prefetch_meta(table()) -> ok | error().
  # prefetch_meta(Table) ->
  #     gen_server:call(server(), {prefetch_meta, Table}).
  #
  # -spec get(table(), rowkey()) -> {ok, hbase_tuples()} | error().
  # get(Table, Key) ->
  #     gen_server:call(server(), {get, Table, Key}).
  #
  # -spec get(table(), rowkey(), cf()) -> {ok, hbase_tuples()} | error().
  # get(Table, Key, CF) ->
  #     gen_server:call(server(), {get, Table, Key, CF}).
  #
  # -spec get(table(), rowkey(), cf(), qualifier()) -> {ok, hbase_tuples()} | error().
  # get(Table, Key, CF, Qualifier) ->
  #     gen_server:call(server(), {get, Table, Key, CF, Qualifier}).
  #
  # -spec scan(binary(), scan_opts(), reference()) -> ok | error().
  # scan(Table, Opts, Ref) ->
  #     gen_server:call(server(), {scan, Table, Opts, Ref}).
  #
  # -spec scan_sync(binary(), scan_opts()) -> {ok, [hbase_tuples()]} | error().
  # scan_sync(Table, Opts) ->
  #     Ref = make_ref(),
  #     ok = scan(Table, Opts, Ref),
  #     receive_scan(Ref).
  #
  # receive_scan(Ref) ->
  #     receive_scan(Ref, []).
  #
  # receive_scan(Ref, Acc) ->
  #     receive
  #         {Ref, row, Row} ->
  #             receive_scan(Ref, [Row | Acc]);
  #         {Ref, done} ->
  #             {ok, lists:reverse(Acc)};
  #         {Ref, error, _, _, _} ->
  #             {error, internal}
  #     after 5000 -> {error, timeout}
  #     end.
  #
  # -spec put(table(), rowkey(), cf(), [qualifier()], [value()]) -> {ok, list()}.
  # put(Table, Key, CF, Qualifiers, Values) ->
  #     gen_server:call(server(), {put, {Table, Key, CF, Qualifiers, Values}}).
  #
  # -spec compare_and_set(table(), rowkey(), cf(), qualifier(), value(), value()) -> {ok, true | false}.
  # compare_and_set(Table, Key, CF, Qualifier, Value, Expected) ->
  #     gen_server:call(server(), {compare_and_set, {Table, Key, CF, [Qualifier], [Value]}, Expected}).
  #
  # -spec increment(table(), rowkey(), cf(), qualifier()) -> {ok, number()}.
  # increment(Table, Key, CF, Qualifier) ->
  #     gen_server:call(server(), {increment, Table, Key, CF, Qualifier}).
  #
  # -spec delete(table(), rowkey()) -> ok.
  # delete(Table, Key) ->
  #     gen_server:call(server(), {delete, Table, Key}).
  #
  # -spec delete(table(), rowkey(), cf()) -> ok.
  # delete(Table, Key, CF) ->
  #     gen_server:call(server(), {delete, Table, Key, CF}).
  #
  # -spec delete(table(), rowkey(), cf(), [qualifier()]) -> ok.
  # delete(Table, Key, CF, Qualifiers) ->
  #     gen_server:call(server(), {delete, Table, Key, CF, Qualifiers}).



  @doc """
  Returns a snapshot of usage statistics for this client.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#stats()
  """
  def client_stats(timeout \\ 5000) do
    server = get_java_server()
    GenServer.call(server, {:client_stats}, timeout)
  end

  # -spec delete(table(), rowkey()) -> ok.
  # delete(Table, Key) ->
  #     gen_server:call(server(), {delete, Table, Key}).
  #
  # -spec delete(table(), rowkey(), cf()) -> ok.
  # delete(Table, Key, CF) ->
  #     gen_server:call(server(), {delete, Table, Key, CF}).
  #
  # -spec delete(table(), rowkey(), cf(), [qualifier()]) -> ok.
  # delete(Table, Key, CF, Qualifiers) ->
  #     gen_server:call(server(), {delete, Table, Key, CF, Qualifiers}).
  @doc """
  Deletes data from HBase.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#delete(org.hbase.async.DeleteRequest)
  """
  def delete4(table, key, family, qualifiers, timeout \\ 5000)
    when is_binary(table) and is_binary(key) and is_binary(family) and
         is_list(qualifiers) and is_integer(timeout) do
    server = get_java_server()
    GenServer.call(server, {:delete, table, key, family}, timeout)
  end
  
  def delete3(table, key, family, timeout \\ 5000)
    when is_binary(table) and is_binary(key) and
         is_binary(family) and is_integer(timeout) do
    server = get_java_server()
    GenServer.call(server, {:delete, table, key, family}, timeout)
  end

  def delete(table, key, timeout \\ 5000)
    when is_binary(table) and is_binary(key) and is_integer(timeout) do
    server = get_java_server()
    GenServer.call(server, {:delete, table, key}, timeout)
  end
  
  @doc """
  Ensures that a given table really exists.

  It's recommended to call this method in the startup code of your application
  if you know ahead of time which tables / families you're going to need, because
  it'll allow you to "fail fast" if they're missing.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#ensureTableExists(byte[])
  """
  def ensure_table_exists(table, timeout \\ 5000) do
    server = get_java_server()
    GenServer.call(server, {:ensure_table_exists, table}, timeout)
  end

  @doc """
  Ensures that a given table/family pair really exists.

  It's recommended to call this method in the startup code of your application
  if you know ahead of time which tables / families you're going to need, because
  it'll allow you to "fail fast" if they're missing.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#ensureTableFamilyExists(byte[],%20byte[])
  """
  def ensure_table_family_exists(table, family, timeout \\ 5000) do
    server = get_java_server()
    GenServer.call(server, {:ensure_table_family_exists, table, family}, timeout)
  end

  @doc """
  Flushes to HBase any buffered client-side write operation.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#flush()
  """
  def flush(timeout \\ 5000) do
    server = get_java_server()
    GenServer.call(server, {:flush}, timeout)
  end
  
  # -spec get(table(), rowkey()) -> {ok, hbase_tuples()} | error().
  # get(Table, Key) ->
  #     gen_server:call(server(), {get, Table, Key}).
  #
  # -spec get(table(), rowkey(), cf()) -> {ok, hbase_tuples()} | error().
  # get(Table, Key, CF) ->
  #     gen_server:call(server(), {get, Table, Key, CF}).
  #
  # -spec get(table(), rowkey(), cf(), qualifier()) -> {ok, hbase_tuples()} | error().
  # get(Table, Key, CF, Qualifier) ->
  #     gen_server:call(server(), {get, Table, Key, CF, Qualifier}).
  #
  @doc """
  Retrieves data from HBase.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#get(org.hbase.async.GetRequest)
  """
  def get(table, key, timeout \\ 5000)
    when is_binary(table) and is_binary(key) do
    server = get_java_server()
    GenServer.call(server, {:get, table, key}, timeout)
  end

  def get3(table, key, family, timeout \\ 5000)
    when is_binary(table) and is_binary(key) and is_binary(family) do
    server = get_java_server()
    GenServer.call(server, {:get, table, key, family}, timeout)
  end

  def get4(table, key, family, qualifier, timeout \\ 5000)
    when is_binary(table) and is_binary(key) and
         is_binary(family) and is_binary(qualifier) do
    server = get_java_server()
    GenServer.call(server, {:get, table, key, family, qualifier}, timeout)
  end

  @doc """
  Returns the maximum time (in milliseconds) for which edits can be buffered.

  The default value is unspecified and implementation dependant, but is guaranteed
  to be non-zero. A return value of 0 indicates that edits are sent directly to
  HBase without being buffered.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#getFlushInterval()
  """
  def get_flush_interval(timeout \\ 5000) do
    server = get_java_server()
    GenServer.call(server, {:get_conf, :flush_interval}, timeout)
  end

  @doc """
  Returns the capacity of the increment buffer.

  Note this returns the capacity of the buffer, not the number of items currently
  in it. There is currently no API to get the current number of items in it.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#getIncrementBufferSize()
  """
  def get_increment_buffer_size(timeout \\ 5000) do
    server = get_java_server()
    GenServer.call(server, {:get_conf, :increment_buffer_size}, timeout)
  end

  @doc """
  Eagerly prefetches and caches a table's region metadata from HBase.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#prefetchMeta(byte[])
  """
  def prefetch_meta(table, timeout \\ 5000) do
    server = get_java_server()
    GenServer.call(server, {:prefetch_meta, table}, timeout)
  end
  
  # -type scan_opts() :: [scan_opt()].
  # -type scan_opt() :: {num_rows, integer()}
  #     | {family, binary()}
  #     | {key_regexp, binary()}
  #     | {max_num_bytes, integer()}
  #     | {max_num_keyvalues, integer()}
  #     | {max_num_rows, integer()}
  #     | {max_timestamp, integer()}
  #     | {max_versions, integer()}
  #     | {qualifier, integer()}
  #     | {server_block_cache, integer()}
  #     | {start_key, binary()}
  #     | {stop_key, binary()}
  #     | {time_range, integer(), integer()}
  #     | {filter, filter_opts()}.
  #
  # -type filter_opts() :: [filter_opt()].
  # -type filter_opt() :: {column_prefix, binary()}
  #     | {column_range, binary(), binary()}
  #     | {first_key_only}
  #     | {fuzzy_row, [{binary(), binary()}]}
  #     | {key_only}
  #     | {key_regexp, binary()}.
  
  def scan(table, ref, opts) do
    timeout = opts |> Keyword.get(:timeout, 5000)
    server = get_java_server()
    GenServer.call(server, {:scan, table, opts, ref}, timeout)
  end

  def scan_sync(table, opts) do
    ref = :erlang.make_ref
    :ok = scan(table, ref, opts)
    receive_scan(ref, [])
  end
  
  defp receive_scan(ref, acc) do
    receive do
      {^ref, :row, row} ->
        receive_scan(ref, [row | acc])
      {^ref, :done} ->
        Enum.reverse(acc)
      {^ref, :error, _, _, _}=e ->
        Logger.error "error: #{inspect e}"
        {:error, :internal}
    after 5000 ->
      {:error, :timeout}
    end
  end

  @doc """
  Stores data in HBase.

  __Note:__ This operation provides no guarantee as to the order in which subsequent
  put requests are going to be applied to the backend. If you need ordering, you
  must enforce it manually yourself.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#put(org.hbase.async.PutRequest)
  """
  def put(table, key, family, qualifier, value, timeout \\ 5000)
  def put(table, key, family, qualifier, value, timeout)
    when is_binary(qualifier) and is_binary(value) do
    put(table, key, family, [qualifier], [value], timeout)
  end
  def put(table, key, family, qualifiers, values, timeout)
    when is_list(qualifiers) and is_list(values) do
    server = get_java_server()
    GenServer.call(server, {:put, {table, key, family, qualifiers, values}}, timeout)
  end

  @doc """
  Sets the maximum time (in milliseconds) for which edits can be buffered.

  This interval will be honored on a "best-effort" basis. Edits can be buffered
  for longer than that due to GC pauses, the resolution of the underlying timer,
  thread scheduling at the OS level (particularly if the OS is overloaded with
  concurrent requests for CPU time), any low-level buffering in the TCP/IP stack
  of the OS, etc.

  Setting a longer interval allows the code to batch requests more efficiently
  but puts you at risk of greater data loss if the JVM or machine was to fail.
  It also entails that some edits will not reach HBase until a longer period of
  time, which can be troublesome if you have other applications that need to read
  the "latest" changes.

  Setting this interval to 0 disables this feature.

  The change is guaranteed to take effect at most after a full interval has elapsed,
  _using the previous interval_ (which is returned).

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#setFlushInterval(short)
  """
  def set_flush_interval(interval, timeout \\ 5000) do
    server = get_java_server()
    GenServer.call(server, {:set_conf, :flush_interval, interval}, timeout)
  end

  @doc """
  Changes the size of the increment buffer.

  __Note:__ Because there is no way to resize the existing buffer, this method
  will flush the existing buffer and create a new one. This side effect might be
  unexpected but is unfortunately required.

  This determines the maximum number of counters this client will keep in-memory
  to allow increment coalescing through `buffer_atomic_increment/4`.

  The greater this number, the more memory will be used to buffer increments, and
  the more efficient increment coalescing can be if you have a high-throughput
  application with a large working set of counters.

  If your application has excessively large keys or qualifiers, you might consider
  using a lower number in order to reduce memory usage.

  See http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html#setIncrementBufferSize(int)
  """
  def set_increment_buffer_size(size, timeout \\ 5000) do
    server = get_java_server()
    GenServer.call(server, {:set_conf, :increment_buffer_size, size}, timeout)
  end

  @doc false
  defp get_java_server() do
    java_node = "__diver__" <> Atom.to_string(Kernel.node())
    {:diver_java_server, String.to_atom(java_node)}
  end

end
