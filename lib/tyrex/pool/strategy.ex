defmodule Tyrex.Pool.Strategy do
  @moduledoc """
  Behaviour for pool dispatch strategies.

  Implement this behaviour to create custom routing logic for `Tyrex.Pool`.

  ## Example

      defmodule MyApp.LeastLoaded do
        @behaviour Tyrex.Pool.Strategy

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
  """

  @doc """
  Return the routing key of the runtime to use.

  `PartitionSupervisor` routes an integer key with `rem(abs(key), size)` and any other term with
  `:erlang.phash2(key, size)`, and documents that both rules may change. A strategy therefore
  returns a key and never a runtime index: the same key always reaches the same runtime, which
  runtime it is is not part of the contract.

  `opts` are the options of the `Tyrex.Pool.eval/3` call being dispatched, so a strategy can
  route on `:key`, on the runtimes themselves, or on anything else the caller passes.

  A strategy keeps no state of its own: the pool is a `PartitionSupervisor`, which has nowhere to
  hold it, and a counter would need a process to own it.
  """
  @callback select(pool_name :: atom(), size :: pos_integer(), opts :: keyword()) :: term()
end
