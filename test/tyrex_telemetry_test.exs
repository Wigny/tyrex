defmodule TyrexTelemetryTest do
  use ExUnit.Case, async: false

  defp attach(events) do
    handler_id = :telemetry_test.attach_event_handlers(self(), events)
    on_exit(fn -> :telemetry.detach(handler_id) end)
    handler_id
  end

  describe "eval span" do
    setup do
      %{pid: start_supervised!({Tyrex, permissions: :none})}
    end

    test "emits a start and a stop event around an eval", %{pid: pid} do
      ref = attach([[:tyrex, :eval, :start], [:tyrex, :eval, :stop]])

      assert {:ok, 3} = Tyrex.eval("1 + 2", pid: pid)

      assert_receive {[:tyrex, :eval, :start], ^ref, start_measurements, start_metadata}
      assert %{system_time: system_time, monotonic_time: _} = start_measurements
      assert is_integer(system_time)
      assert %{runtime: ^pid, pool: nil, index: nil, blocking: false} = start_metadata

      assert_receive {[:tyrex, :eval, :stop], ^ref, stop_measurements, stop_metadata}
      assert %{duration: duration} = stop_measurements
      assert duration > 0
      assert %{result: :ok} = stop_metadata
    end

    test "reports the error name as the result of a failed eval", %{pid: pid} do
      ref = attach([[:tyrex, :eval, :stop]])

      assert {:error, %Tyrex.Error{name: :execution_error}} =
               Tyrex.eval("nonExistentVariable", pid: pid)

      assert_receive {[:tyrex, :eval, :stop], ^ref, _measurements, %{result: :execution_error}}
    end

    test "reports a blocking eval as blocking", %{pid: pid} do
      ref = attach([[:tyrex, :eval, :start]])

      assert {:ok, 3} = Tyrex.eval("1 + 2", pid: pid, blocking: true)

      assert_receive {[:tyrex, :eval, :start], ^ref, _measurements, %{blocking: true}}
    end
  end

  describe "eval span in a pool" do
    test "names the pool and the index of the runtime the strategy picked" do
      ref = attach([[:tyrex, :eval, :start]])
      start_supervised!({Tyrex.Pool, name: :telemetry_pool, size: 2, permissions: :none})

      assert {:ok, 3} = Tyrex.Pool.eval(:telemetry_pool, "1 + 2")

      assert_receive {[:tyrex, :eval, :start], ^ref, _measurements, metadata}
      assert %{pool: :telemetry_pool, index: index} = metadata
      assert index in 0..1
      assert metadata.runtime == :"telemetry_pool.Runtime.#{index}"
    end
  end

  describe "runtime memory event" do
    test "emits one event per runtime of the pool, carrying V8's byte counts" do
      ref = attach([[:tyrex, :runtime, :memory]])
      start_supervised!({Tyrex.Pool, name: :telemetry_memory_pool, size: 2, permissions: :none})

      assert :ok = Tyrex.Pool.measure_memory(:telemetry_memory_pool)

      for index <- 0..1 do
        assert_receive {[:tyrex, :runtime, :memory], ^ref, measurements, metadata}
        assert %{used_heap_size: used, heap_size_limit: limit} = measurements
        assert used > 0
        assert used < limit
        assert %{pool: :telemetry_memory_pool, index: ^index} = metadata
        assert metadata.runtime == :"telemetry_memory_pool.Runtime.#{index}"
      end
    end
  end

  describe "runtime lifecycle events" do
    test "emits a start event once the runtime is usable" do
      ref = attach([[:tyrex, :runtime, :start]])

      start_supervised!({Tyrex, name: :telemetry_started, permissions: :none})

      assert_receive {[:tyrex, :runtime, :start], ^ref, measurements, metadata}
      assert %{system_time: system_time, duration: duration} = measurements
      assert is_integer(system_time)
      assert duration > 0
      assert %{runtime: :telemetry_started, pool: nil, index: nil} = metadata
    end

    test "emits a stop event carrying the lifetime and the stop reason" do
      pid =
        start_supervised!({Tyrex, name: :telemetry_stopped, permissions: :none},
          restart: :temporary
        )

      ref = attach([[:tyrex, :runtime, :stop]])

      assert :ok = Tyrex.stop(pid: pid)

      assert_receive {[:tyrex, :runtime, :stop], ^ref, measurements, metadata}
      assert %{duration: duration} = measurements
      assert duration > 0
      assert %{runtime: :telemetry_stopped, reason: :normal} = metadata
    end

    test "names the pool and the index of a pooled runtime" do
      ref = attach([[:tyrex, :runtime, :start]])

      start_supervised!(
        {Tyrex.Pool, name: :telemetry_lifecycle_pool, size: 1, permissions: :none}
      )

      assert_receive {[:tyrex, :runtime, :start], ^ref, _measurements, metadata}
      assert %{pool: :telemetry_lifecycle_pool, index: 0} = metadata
    end
  end
end
