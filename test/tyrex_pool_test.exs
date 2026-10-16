defmodule TyrexPoolTest do
  use ExUnit.Case, async: false

  # Runtimes are unregistered partitions, so a test reaches one through the pool's own routing.
  # An integer key below the pool size lands on that partition under the current routing rule,
  # which `PartitionSupervisor` documents as changeable — fine to lean on in a test, never in
  # `lib/`.
  defp runtime_pid(pool, index) do
    GenServer.whereis({:via, PartitionSupervisor, {pool, index}})
  end

  defp runtime_pids(pool, size) do
    for index <- 0..(size - 1), do: runtime_pid(pool, index)
  end

  describe "pool basics" do
    test "start a pool and eval" do
      {:ok, _} = Tyrex.Pool.start_link(name: :basic_pool, size: 2)
      assert {:ok, 3} = Tyrex.Pool.eval(:basic_pool, "1 + 2")
      assert {:ok, "hello"} = Tyrex.Pool.eval(:basic_pool, "'hello'")
      Supervisor.stop(:basic_pool)
    end

    test "pool defaults size to schedulers_online" do
      {:ok, sup} = Tyrex.Pool.start_link(name: :default_size_pool)
      assert PartitionSupervisor.partitions(:default_size_pool) == System.schedulers_online()
      Supervisor.stop(sup)
    end

    test "pool with explicit size" do
      {:ok, sup} = Tyrex.Pool.start_link(name: :sized_pool, size: 3)
      assert PartitionSupervisor.partitions(:sized_pool) == 3
      assert Enum.all?(runtime_pids(:sized_pool, 3), &is_pid/1)
      Supervisor.stop(sup)
    end

    test "eval! returns unwrapped value" do
      {:ok, _} = Tyrex.Pool.start_link(name: :bang_pool, size: 1)
      assert 42 = Tyrex.Pool.eval!(:bang_pool, "42")
      Supervisor.stop(:bang_pool)
    end
  end

  describe "default strategy" do
    test "spreads calls over every runtime" do
      {:ok, _} = Tyrex.Pool.start_link(name: :spread_pool, size: 2)

      # Each runtime marks itself on first contact, so the number of distinct markers is the
      # number of runtimes the strategy reached.
      markers =
        for _ <- 1..20 do
          {:ok, marker} = Tyrex.Pool.eval(:spread_pool, "globalThis.marker ??= Math.random()")
          marker
        end

      assert markers |> Enum.uniq() |> length() == 2

      Supervisor.stop(:spread_pool)
    end
  end

  describe "random strategy" do
    test "selects randomly" do
      {:ok, _} =
        Tyrex.Pool.start_link(
          name: :rand_pool,
          size: 4,
          strategy: Tyrex.Pool.Strategy.Random
        )

      # Just verify it works (can't test randomness deterministically)
      results =
        for _ <- 1..20 do
          {:ok, result} = Tyrex.Pool.eval(:rand_pool, "1 + 1")
          result
        end

      assert Enum.all?(results, &(&1 == 2))

      Supervisor.stop(:rand_pool)
    end
  end

  describe "hash strategy" do
    test "same key always hits same runtime" do
      {:ok, _} =
        Tyrex.Pool.start_link(
          name: :hash_pool,
          size: 4,
          strategy: Tyrex.Pool.Strategy.Hash
        )

      # Set state with a specific key
      Tyrex.Pool.eval(:hash_pool, "globalThis.session = Math.random()", key: "user_123")

      # Same key should always get the same value back
      {:ok, value} = Tyrex.Pool.eval(:hash_pool, "globalThis.session", key: "user_123")

      for _ <- 1..10 do
        assert {:ok, ^value} =
                 Tyrex.Pool.eval(:hash_pool, "globalThis.session", key: "user_123")
      end

      Supervisor.stop(:hash_pool)
    end

    test "different keys may hit different runtimes" do
      {:ok, _} =
        Tyrex.Pool.start_link(
          name: :hash_pool2,
          size: 4,
          strategy: Tyrex.Pool.Strategy.Hash
        )

      # Set unique values per key using key-specific variable names
      for i <- 1..20 do
        Tyrex.Pool.eval(:hash_pool2, "globalThis.val_key_#{i} = #{i}", key: "key_#{i}")
      end

      # Verify sticky sessions - same key hits same runtime with key-specific var
      for i <- 1..20 do
        {:ok, val} = Tyrex.Pool.eval(:hash_pool2, "globalThis.val_key_#{i}", key: "key_#{i}")
        assert val == i
      end

      Supervisor.stop(:hash_pool2)
    end

    test "no key falls back to random" do
      {:ok, _} =
        Tyrex.Pool.start_link(
          name: :hash_pool3,
          size: 2,
          strategy: Tyrex.Pool.Strategy.Hash
        )

      # Without key, should still work
      assert {:ok, 5} = Tyrex.Pool.eval(:hash_pool3, "2 + 3")

      Supervisor.stop(:hash_pool3)
    end
  end

  describe "pool with main module" do
    test "all runtimes share the same main module" do
      {:ok, _} =
        Tyrex.Pool.start_link(
          name: :module_pool,
          size: 2,
          main_module_path: "test/support/main_module.js"
        )

      # Both runtimes should have the module loaded
      assert {:ok, 5} = Tyrex.Pool.eval(:module_pool, "addNumbers(2, 3)")
      assert {:ok, 7} = Tyrex.Pool.eval(:module_pool, "addNumbers(3, 4)")

      Supervisor.stop(:module_pool)
    end
  end

  # `Tyrex.Pool.start_link/1` re-lists the options it hands to each child, so a runtime option can
  # exist on `Tyrex.start_link/1` and never reach a pooled runtime — `:apply` and `:max_heap_mb`
  # were dropped exactly that way until v0.4.0, and a pooled runtime has no other route to a heap
  # cap.
  describe "pool option forwarding" do
    test ":apply reaches every runtime in the pool" do
      {:ok, _} = Tyrex.Pool.start_link(name: :apply_pool, size: 2, apply: [{Enum, :sum, 1}])

      # Ten calls spread over both children: the allowlist has to have reached each of them, not
      # just the one the first eval landed on.
      for _ <- 1..10 do
        assert {:ok, 6} =
                 Tyrex.Pool.eval(
                   :apply_pool,
                   ~s|(async () => await Tyrex.apply("Enum", "sum", [[1,2,3]]))()|
                 )
      end

      # And the allowlist travelled, not merely the bridge: an MFA outside it is
      # refused by the runtime that received the list.
      assert {:error, %Tyrex.Error{name: :promise_rejection, value: value}} =
               Tyrex.Pool.eval(
                 :apply_pool,
                 ~s|(async () => await Tyrex.apply("File", "read!", ["mix.exs"]))()|
               )

      assert value =~ "permission_denied"

      Supervisor.stop(:apply_pool)
    end

    test "a pool started without :apply has no bridge in its runtimes" do
      {:ok, _} = Tyrex.Pool.start_link(name: :no_apply_pool, size: 2)

      # The negative case that makes the positive one mean something: without
      # `:apply` the bootstrap deletes the global, so a bridge observed above
      # can only have come from the forwarded allowlist.
      for _ <- 1..10 do
        assert {:ok, "undefined"} = Tyrex.Pool.eval(:no_apply_pool, "typeof globalThis.Tyrex")
      end

      Supervisor.stop(:no_apply_pool)
    end

    @tag timeout: 120_000
    test ":max_heap_mb reaches a pooled runtime and caps it" do
      {:ok, sup} =
        Tyrex.Pool.start_link(
          name: :heap_pool,
          size: 1,
          permissions: :none,
          max_heap_mb: 64
        )

      code = """
      const chunks = [];
      for (;;) { chunks.push(new Array(1_000_000).fill(7)); }
      """

      # Pinned by equality. If the cap never reached the child the guest just
      # allocates until the eval deadline and reports `:timeout`, which would
      # leave this green over a pool whose runtimes have no heap limit at all.
      assert {:error, %Tyrex.Error{name: :heap_limit_error}} =
               Tyrex.Pool.eval(:heap_pool, code, timeout: 30_000)

      Supervisor.stop(sup)
    end
  end

  describe "pool concurrency" do
    test "handles concurrent requests" do
      {:ok, _} = Tyrex.Pool.start_link(name: :conc_pool, size: 4)

      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            {:ok, result} = Tyrex.Pool.eval(:conc_pool, "#{i} * 3")
            result
          end)
        end

      results = Task.await_many(tasks, 10_000)
      assert Enum.sort(results) == Enum.map(1..50, &(&1 * 3))

      Supervisor.stop(:conc_pool)
    end
  end

  describe "pool cleanup" do
    test "stopping a pool takes its routing table with it" do
      {:ok, sup} = Tyrex.Pool.start_link(name: :cleanup_pool, size: 2)

      assert is_reference(:ets.whereis(:cleanup_pool))

      Supervisor.stop(sup)

      refute Process.alive?(sup)
      assert :ets.whereis(:cleanup_pool) == :undefined
    end
  end

  # Making eval deadlines real turned a guest-triggered, caller-local timeout into a supervisor
  # restart event. With the runtimes under a `:rest_for_one` supervisor that was a denial of
  # service reachable from `while (true) {}`: one deadline restarted every runtime ordered after
  # the victim (measured 4/4 for a pool of four), and five deadlines in ~2.5s exhausted the
  # default intensity and took the pool supervisor down with `:shutdown`. Siblings were signalled
  # rather than stopped, so their `terminate/2` — and the in-flight drain it performs — was
  # skipped, and `{:shutdown, _}` terminations are not logged, so the churn was silent.
  # `PartitionSupervisor` is `:one_for_one`, which is what keeps this green.
  describe "one guest cannot take out the pool" do
    @tag timeout: 120_000
    test "a deadline on one runtime leaves its siblings untouched" do
      {:ok, sup} = Tyrex.Pool.start_link(name: :blast_pool, size: 4, permissions: :none)
      [victim | siblings] = runtime_pids(:blast_pool, 4)
      assert Enum.all?([victim | siblings], &is_pid/1)

      ref = Process.monitor(victim)
      spawn(fn -> Tyrex.eval("for(;;){}", pid: victim, timeout: 300) end)
      assert_receive {:DOWN, ^ref, :process, ^victim, {:shutdown, :timeout}}, 5_000

      # The victim was replaced...
      assert eventually(fn ->
               pid = runtime_pid(:blast_pool, 0)
               is_pid(pid) and pid != victim
             end)

      # ...and nothing else moved. This is the assertion: under `:rest_for_one`
      # all three siblings had a new pid here.
      for {pid, index} <- Enum.with_index(siblings, 1) do
        assert runtime_pid(:blast_pool, index) == pid,
               "runtime #{index} was restarted by an unrelated runtime's deadline"

        assert Process.alive?(pid)
      end

      Supervisor.stop(sup)
    end

    @tag timeout: 120_000
    test "repeated deadlines do not exhaust the pool supervisor" do
      {:ok, sup} = Tyrex.Pool.start_link(name: :storm_pool, size: 2, permissions: :none)

      # Six is comfortably past the default intensity of 3-in-5s that killed the
      # supervisor before the runtimes were rescoped.
      for _ <- 1..6 do
        victim = runtime_pid(:storm_pool, 0)

        if is_pid(victim) and Process.alive?(victim) do
          ref = Process.monitor(victim)
          spawn(fn -> Tyrex.eval("for(;;){}", pid: victim, timeout: 200) end)
          assert_receive {:DOWN, ^ref, :process, ^victim, _}, 5_000
        end

        assert Process.alive?(sup), "the pool supervisor died after a guest deadline"
      end

      assert eventually(fn -> match?({:ok, 1}, Tyrex.Pool.eval(:storm_pool, "1")) end)

      Supervisor.stop(sup)
    end
  end

  defp eventually(fun, attempts \\ 50)

  defp eventually(_fun, 0), do: false

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(100)
      eventually(fun, attempts - 1)
    end
  end
end
