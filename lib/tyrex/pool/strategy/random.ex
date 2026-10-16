defmodule Tyrex.Pool.Strategy.Random do
  @moduledoc """
  Random dispatch strategy.

  Selects a random runtime for each request. Good for bursty workloads
  where even distribution isn't critical.
  """

  @behaviour Tyrex.Pool.Strategy

  @doc """
  Routes on a fresh random float.

  The key is drawn from a large space, not from `1..size`, so the spread does not depend on
  which partition `PartitionSupervisor` maps a given key to.
  """
  @impl true
  def select(_pool_name, _size, _opts) do
    :rand.uniform()
  end
end
