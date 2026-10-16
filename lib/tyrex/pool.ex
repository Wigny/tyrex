defmodule Tyrex.Pool do
  @moduledoc """
  A pool of Deno runtimes with pluggable dispatch strategies.

  `Tyrex.Pool` is a `PartitionSupervisor` that starts multiple `Tyrex` GenServer children
  and distributes `eval` calls across them using a configurable strategy.

  ## Usage

      # In your supervision tree
      children = [
        {Tyrex.Pool, name: :js_pool, size: 4}
      ]

      # Evaluate code on a pool-selected runtime
      {:ok, result} = Tyrex.Pool.eval(:js_pool, "1 + 2")

  ## Strategies

    * `Tyrex.Pool.Strategy.Random` (default) — picks a random runtime
    * `Tyrex.Pool.Strategy.Hash` — routes by `:key` option for sticky sessions

  See `Tyrex.Pool.Strategy` for implementing custom strategies.

  ## Lifecycle

  The runtimes are unregistered and are addressed through the `PartitionSupervisor`, which keeps
  its routing table under the pool's name. Stopping the pool takes the processes and that table
  with it, so pools can be started and stopped dynamically — one per tenant, for example.
  """

  alias Tyrex.Error

  @doc false
  def child_spec(opts) do
    %{
      id: Keyword.fetch!(opts, :name),
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @doc """
  Start a pool of Tyrex runtimes.

  ## Options

    * `:name` - Required. The name of the pool.
    * `:size` - Number of runtimes. Defaults to `System.schedulers_online()`.
    * `:strategy` - Dispatch strategy module. Defaults to `Tyrex.Pool.Strategy.Random`.
    * `:main_module_path` - Path to the main JS module for all runtimes.
    * `:permissions` - Runtime permissions. See `Tyrex.start/1` for details.
    * `:apply` - Elixir bridge allowlist. See `Tyrex.start/1` for details.
    * `:max_heap_mb` - Per-runtime V8 heap cap. See `Tyrex.start/1` for details.
    * `:max_restarts` / `:max_seconds` - Restart intensity for the runtime
      children. Defaults to `max(size * 4, 12)` restarts in 5 seconds.

  Runtimes are supervised `:one_for_one`, so one runtime hitting its `:timeout`
  or `:max_heap_mb` cap does not disturb its siblings. The default intensity is
  deliberately generous because guest code chooses the failure rate: a
  terminated runtime is ordinary operation here, not a systemic fault.
  """
  @spec start_link(Keyword.t()) :: Supervisor.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    size = Keyword.get(opts, :size, System.schedulers_online())
    strategy_mod = Keyword.get(opts, :strategy, Tyrex.Pool.Strategy.Random)

    runtime_opts =
      Keyword.take(opts, [:main_module_path, :permissions, :startup_timeout, :apply, :max_heap_mb])

    :persistent_term.put({__MODULE__, name}, strategy_mod)

    PartitionSupervisor.start_link(
      name: name,
      partitions: size,
      child_spec: {Tyrex, [name: nil] ++ runtime_opts},
      max_restarts: Keyword.get(opts, :max_restarts, max(size * 4, 12)),
      max_seconds: Keyword.get(opts, :max_seconds, 5)
    )
  end

  @doc """
  Run JavaScript code on a runtime selected by the pool's strategy.

  ## Options

    * `:key` - Dispatch key (used by hash strategy for sticky sessions).
    * `:timeout` - Timeout for the call.
    * `:blocking` - Whether to use blocking mode.
  """
  @spec eval(atom(), binary(), Keyword.t()) :: {:ok, term()} | {:error, Tyrex.Error.t()}
  def eval(pool_name, code, opts \\ []) do
    strategy_mod = :persistent_term.get({__MODULE__, pool_name})
    size = PartitionSupervisor.partitions(pool_name)

    key = strategy_mod.select(pool_name, size, opts)
    Tyrex.eval(code, Keyword.put(opts, :name, {:via, PartitionSupervisor, {pool_name, key}}))
  end

  @doc """
  Same as `eval/3`, but raises `Tyrex.Error` on error.
  """
  @spec eval!(atom(), binary(), Keyword.t()) :: term() | no_return()
  def eval!(pool_name, code, opts \\ []) do
    case eval(pool_name, code, opts) do
      {:ok, result} -> result
      {:error, %Error{} = e} -> raise e
    end
  end
end
