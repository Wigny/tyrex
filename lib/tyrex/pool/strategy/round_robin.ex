defmodule Tyrex.Pool.Strategy.RoundRobin do
  @moduledoc """
  Round-robin dispatch strategy (default).

  Uses an `:atomics` counter for lock-free, sequential cycling through runtimes.
  """

  @behaviour Tyrex.Pool.Strategy

  @impl true
  def init(_pool_name, size) do
    {:atomics.new(1, signed: true), size}
  end

  @doc """
  Selects the next runtime index in 0..size-1 using a lock-free `:atomics`
  counter. Safe to call concurrently from any number of processes.
  """
  @impl true
  def select({ref, size}, _opts) do
    rem(abs(:atomics.add_get(ref, 1, 1) - 1), size)
  end
end
