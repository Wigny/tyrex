defmodule Tyrex.Pool.Strategy.Hash do
  @moduledoc """
  Hash-based dispatch strategy for sticky sessions.

  Routes requests by a `:key` option — the same key always hits the same runtime.
  Useful for stateful JavaScript sessions (e.g., per-user state).

      Tyrex.Pool.eval(:pool, "getCart()", key: user_id)

  If no `:key` is provided, falls back to random selection.
  """

  @behaviour Tyrex.Pool.Strategy

  @doc """
  Routes on `opts[:key]`, which `PartitionSupervisor` hashes to one runtime. Falls back to a
  random key when no `:key` is provided.
  """
  @impl true
  def select(_pool_name, _size, opts) do
    Keyword.get_lazy(opts, :key, fn -> :rand.uniform() end)
  end
end
