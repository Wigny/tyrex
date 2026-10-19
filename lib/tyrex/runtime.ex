defmodule Tyrex.Runtime do
  @moduledoc """
  State of a running `Tyrex` GenServer.

  This is an opaque value managed internally by `Tyrex` — you should not
  create or modify it directly.

  The `:reference` field is the opaque NIF resource handle (a Rustler
  `ResourceArc`) that keeps the underlying Deno worker alive for as long as
  this struct is reachable. Dropping the struct without a matching
  `Tyrex.stop/1` lets the resource be garbage-collected, which terminates the
  isolate and signals the worker to shut down.
  """

  @enforce_keys [
    :reference
  ]

  @typedoc """
  A single entry of the `:apply` allowlist, keyed by the exact strings guest
  JavaScript passes to `Tyrex.apply` and the argument count it supplies.
  """
  @type allowlist :: %{{binary(), binary(), arity()} => {module(), atom()}} | nil

  @typedoc """
  Tyrex runtime state: the opaque NIF resource reference, the resolved
  `Tyrex.apply` allowlist (`nil` when the bridge is disabled), the map of
  in-flight `eval` callers to their deadline timers, and, for its telemetry
  events, the `Tyrex.Pool` name and index of the runtime (`nil` outside a pool)
  and the monotonic time at which it started.
  """
  @type t :: %__MODULE__{
          reference: reference(),
          apply_allowlist: allowlist(),
          inflight: %{GenServer.from() => reference()},
          pool: atom() | nil,
          index: non_neg_integer() | nil,
          started_at: integer() | nil
        }

  defstruct [
    :reference,
    apply_allowlist: nil,
    inflight: %{},
    pool: nil,
    index: nil,
    started_at: nil
  ]
end
