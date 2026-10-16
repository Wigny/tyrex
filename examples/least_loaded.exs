# Least-loaded dispatch strategy
# Run: TYREX_BUILD=true mix run examples/least_loaded.exs
#
# Demonstrates a custom `Tyrex.Pool.Strategy` that picks the runtime with the
# shortest message queue. This is useful when individual evaluations have wildly
# different costs and you'd rather keep busy workers busy than block on them.

defmodule LeastLoaded do
  @moduledoc """
  Picks the `Tyrex` runtime with the smallest mailbox.

  No ETS, no GenServer — just `Process.info/2` on each runtime per select. Fine for small
  pools; consider sampling two runtimes instead of all of them for larger ones.

  A restarting runtime resolves to `nil` and counts as saturated, so a restart does not fail
  the call.
  """

  @behaviour Tyrex.Pool.Strategy

  @impl true
  def select(pool_name, size, _opts) do
    Enum.min_by(1..size, fn key ->
      pid = GenServer.whereis({:via, PartitionSupervisor, {pool_name, key}})

      case Process.info(pid, :message_queue_len) do
        {:message_queue_len, length} -> length
        nil -> :infinity
      end
    end)
  end
end

IO.puts("=== Tyrex Least-Loaded Pool Strategy ===\n")

size = 4
{:ok, _} = Tyrex.Pool.start_link(name: :ll_pool, size: size, permissions: :none, strategy: LeastLoaded)

# Tag each runtime so we can attribute calls to it after the fact.
# Init via the runtime's pid (NOT the pool) so every runtime gets initialized —
# the LeastLoaded strategy would route all four init calls to one runtime since
# it's always the least-loaded when nothing's queued.
runtimes =
  :ll_pool
  |> PartitionSupervisor.which_children()
  |> Enum.map(fn {partition, pid, _type, _modules} -> {partition, pid} end)
  |> Enum.sort()

for {partition, pid} <- runtimes do
  Tyrex.eval("globalThis.runtimeId = #{partition}; globalThis.calls = 0", pid: pid)
end

# Mixed workload: 1/3 "expensive" (a JS setTimeout), 2/3 cheap math.
workload =
  for i <- 1..40 do
    if rem(i, 3) == 0 do
      {:slow,
       "await new Promise(r => setTimeout(r, 200)); globalThis.calls++; return globalThis.runtimeId;"}
    else
      {:fast, "globalThis.calls++; const _ = #{i} * 2; return globalThis.runtimeId;"}
    end
  end

IO.puts("Dispatching #{length(workload)} mixed jobs (1/3 slow, 2/3 fast)...")

started = System.monotonic_time(:millisecond)

results =
  workload
  |> Task.async_stream(
    fn {kind, code} ->
      {:ok, runtime_id} =
        Tyrex.Pool.eval(:ll_pool, "(async () => { #{code} })()", timeout: 10_000)

      {kind, runtime_id}
    end,
    max_concurrency: size * 2,
    timeout: 15_000
  )
  |> Enum.map(fn {:ok, pair} -> pair end)

elapsed = System.monotonic_time(:millisecond) - started

distribution =
  results
  |> Enum.group_by(fn {_kind, runtime_id} -> runtime_id end)
  |> Enum.map(fn {runtime_id, jobs} ->
    {runtime_id,
     %{
       total: length(jobs),
       slow: Enum.count(jobs, fn {k, _} -> k == :slow end),
       fast: Enum.count(jobs, fn {k, _} -> k == :fast end)
     }}
  end)
  |> Enum.sort_by(fn {runtime_id, _} -> runtime_id end)

IO.puts("\nElapsed: #{elapsed}ms\n")
IO.puts("Per-runtime breakdown:")

for {runtime_id, %{total: total, slow: slow, fast: fast}} <- distribution do
  IO.puts("  runtime #{runtime_id}: total=#{total}  slow=#{slow}  fast=#{fast}")
end

# Cross-check via JS-side counter — should match the totals above.
IO.puts("\nJS-side `globalThis.calls` per runtime:")

for {partition, pid} <- runtimes do
  {:ok, calls} = Tyrex.eval("globalThis.calls", pid: pid)
  IO.puts("  runtime #{partition}: calls=#{calls}")
end

Supervisor.stop(:ll_pool)

IO.puts("\n=== Done! ===")
