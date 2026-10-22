defmodule Tyrex.Telemetry do
  @moduledoc false
  # Internal: the single place that builds and emits Tyrex's telemetry events.
  # The events themselves are public API and are documented in the README.

  alias Tyrex.Error
  alias Tyrex.Runtime

  @doc """
  Emit `[:tyrex, :runtime, :start]` for a runtime that has just become usable.

  `:duration` is how long the runtime took to start.
  """
  @spec runtime_start(Runtime.t()) :: :ok
  def runtime_start(%Runtime{} = runtime) do
    :telemetry.execute(
      [:tyrex, :runtime, :start],
      %{
        system_time: System.system_time(),
        duration: System.monotonic_time() - runtime.started_at
      },
      metadata(runtime)
    )
  end

  @doc """
  Emit `[:tyrex, :runtime, :stop]` for a runtime that is ending, with its lifetime and `reason`.

  Emit it from the runtime's own shutdown path, where the reason is known. A death that skips
  that path — `Process.exit(pid, :kill)`, or a supervisor shutdown of a process that does not
  trap exits — goes unreported.
  """
  @spec runtime_stop(Runtime.t(), term()) :: :ok
  def runtime_stop(%Runtime{} = runtime, reason) do
    :telemetry.execute(
      [:tyrex, :runtime, :stop],
      %{duration: System.monotonic_time() - runtime.started_at},
      Map.put(metadata(runtime), :reason, reason)
    )
  end

  @doc """
  Emit `[:tyrex, :runtime, :memory]` with V8's heap statistics as the measurements.

  The event is pull-based: reading the statistics costs a round trip to the isolate's own
  thread, so nothing emits it on its own and the host drives it from its own
  `:telemetry_poller`. The read therefore happens in the poller's process rather than the
  runtime's, which is why the caller supplies the metadata instead of `metadata/1` deriving
  it from `self/0`.
  """
  @spec runtime_memory(:telemetry.event_metadata(), :telemetry.event_measurements()) :: :ok
  def runtime_memory(metadata, statistics) do
    :telemetry.execute([:tyrex, :runtime, :memory], statistics, metadata)
  end

  @doc """
  Run `fun` as a `[:tyrex, :eval]` span and return its result unchanged.

  Wrap the whole call to the runtime, so that the span measures the wait in the runtime's
  mailbox as well as the evaluation. The `:stop` metadata gains `:result` — `:ok`, or the
  `Tyrex.Error` name of the failure.
  """
  @spec eval_span(:telemetry.event_metadata(), (-> result)) :: result when result: var
  def eval_span(metadata, fun) do
    :telemetry.span([:tyrex, :eval], metadata, fn ->
      result = fun.()

      {result, Map.put(metadata, :result, eval_result(result))}
    end)
  end

  defp eval_result({:ok, _value}), do: :ok
  defp eval_result({:error, %Error{name: name}}), do: name

  defp metadata(runtime) do
    name =
      case Process.info(self(), :registered_name) do
        {:registered_name, name} when is_atom(name) -> name
        _unregistered -> self()
      end

    %{runtime: name, pool: runtime.pool, index: runtime.index}
  end
end
